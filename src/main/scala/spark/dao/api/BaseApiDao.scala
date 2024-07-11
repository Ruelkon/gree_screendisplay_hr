package spark.dao.api

import akka.actor.{Actor, ActorRef, Props, Terminated}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.common.Constants
import spark.common.config.ApiConfigTrait
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{ActorUtil, DataFrameUtil, DateUtil, HttpRequest, HttpUtils}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/11/10
 * Time: 19:25
 * Created by: 聂嘉良
 */

/**
 * 获取接口数据的基础模板
 * @param db 要存入的库名
 * @param tableName 表名
 * @param gradient 每个线程一次取多少页
 * @param tableType hive/kudu
 * @param pageSize 每页的数据量
 */
abstract class BaseApiDao(val db: String, val tableName: String, val gradient: Int, val tableType: String = "hive", val pageSize: Int = 1000) {

  //日志类对象，用于打印日志
  val logger: Logger = Logger.getLogger(getClass)

  protected val httpUtil = new HttpUtils()

  val client: CloseableHttpClient = httpUtil.createSSLClientDefault()

  val spark: SparkSession

  val conf: ApiConfigTrait

  var finish = false

  /**
   * jsonString转为dataframe前数据清洗
   * @param result 将要转为dataframe的jsonString
   * @return
   */
  def resultClean(result: String): String = {
    //    val charArray = result.toCharArray
    Try(result
      .replaceAll("(t\\$(date|full|conf|user))", "t_$2")
      .replaceAll("(T\\$(DATE|FULL|CONF|USER))", "T_$2")
      .replaceAll("([tT]\\$)", "")
      .replaceAll("\\$", "_")
      .replaceAll("[\n]|[\r]|[\t]", "")  //数据清洗，去掉制表符
      .replaceAll(":\" *(\\S+?) *\"", ":\"$1\"")  // 去掉左右空格
      .replaceAll(":\"\"", ":\"null\"")  // 将""值替换为"null"值
    ) match {
      case Success(value) => value
      case Failure(exception) =>
        logger.info("数据异常，" + result)
        exception.printStackTrace()
        ""
    }
  }

  /**
   * 获取请求每一页的request
   */
  def getRequestEachPage(page: Int, socketTimeout: Int = 1000*60*20, connectTimeout: Int = 1000*60*5, connectionRequestTimeout: Int = 1000*60*5): HttpRequest

  /**
   * 获取从startPage到startPage+gradient 的所有request，返回为list形式
   */
  def getRequestListMutiPage(startPage: Int): ListBuffer[HttpRequest] ={
    val requestList = new ListBuffer[HttpRequest]()
    for(page <- startPage until (startPage+gradient)){
      requestList.append(getRequestEachPage(page))
    }
    requestList
  }

  /**
   * 取出原始json中的真正数据
   */
  def getDataInJson(jsonResult: String): String

  /**
   * 获取多个page的数据，再合并为jsonList
   */
  def getJsonMutiPage(startPage: Int, actorRef: ActorRef = null): ListBuffer[String] = {
    val requestList = getRequestListMutiPage(startPage)
    //    logger.info(s"post page: $startPage - ${startPage+gradient-1}" )
    val resultList = httpUtil.requestAll(client, requestList.toArray, "UTF-8")
    val jsonList = new ListBuffer[String]

    resultList.foreach(result => {
      Try[String](getDataInJson(result)) match {
        case Success(value) if value != "[]" =>
          // logger.info(value)
          jsonList.append(resultClean(value))
        case Success(value) if value == "[]" =>
          logger.info("接口返回空数据：" + result)
        case _ =>
          logger.info("接口返回异常结果，该异常结果为：" + result)
      }
    })
    if(!getClass.getSimpleName.equals("MesApi")){
      if(jsonList.isEmpty){
        finish = true
        if(actorRef != null)
          actorRef ! "stop"
      }
    }
    jsonList
  }

  /**
   * 将获取到的jsonList存入hdfs
   */
  def saveJsonMutiPage(currentPage: Int, actorRef: ActorRef = null): Unit ={
    val jsonMutiPage: ListBuffer[String] = getJsonMutiPage(currentPage, actorRef)
    if(jsonMutiPage.nonEmpty) {
      val save_path = Constants.HDFS_URL+s"/user/hive/warehouse/$db.db/${tableName}_tmp/$currentPage"
      spark.sparkContext.parallelize(jsonMutiPage).repartition(1).saveAsTextFile(save_path)
      logger.info(s"saved_page: $currentPage ~ ${currentPage+gradient-1}  【$db.${tableName}_tmp】->$save_path at ${DateUtil.getTimeNow} 保存完成")
    }
  }

  /**
   * 并发写入某个api的全量数据，以多次调用getJsonMutiPage的形式
   */
  def actorSaveApiJsonFull(api: BaseApiDao, startPage: Int = 1, isConcurrent: Boolean = true): Unit = {
    val actorUtil = new ActorUtil
    val hiveDao = new HiveDaoImpl(spark)
    val dataPath = Constants.HDFS_URL+s"/user/hive/warehouse/${api.db}.db/${api.tableName}_tmp/"
    hiveDao.deleteHdfs(dataPath)

    val controllerActorRef = actorUtil.actorFactory.actorOf(Props(new ControllerActor(api, actorUtil)))
    controllerActorRef ! (isConcurrent, startPage, "saveJson")
    Await.result(actorUtil.actorFactory.whenTerminated, Duration.Inf)
    api.client.close()

    Try[DataFrame](spark.read.json(dataPath+"*")) match {
      case Success(df) =>
        val tableResult = DataFrameUtil.cleanSchema(df, spark)
        hiveDao.write(api.db, api.tableName, tableResult.repartition(1), SaveMode.Overwrite)
        spark.sql(s"use ${api.db}")
        if (!spark.sql(s"show tables like '${api.tableName}'").isEmpty)
          hiveDao.deleteHdfs(dataPath)
      case Failure(exception) =>
        exception.printStackTrace()
    }

  }

  class ControllerActor(api: BaseApiDao, actorUtil: ActorUtil) extends Actor{
    override def receive: Receive = {
      case (isConcurrent: Boolean, startPage: Int, "saveJson") =>
        val saveApiFullJsonActorRef = actorUtil.actorFactory.actorOf(Props(new SaveApiJsonFullActor(api, actorUtil)))
        saveApiFullJsonActorRef ! (isConcurrent, startPage)
        context.watch(saveApiFullJsonActorRef)

      case Terminated(_) =>
        actorUtil.actorFactory.terminate()
        logger.info("关闭ActorFactory")
    }
  }

  class SaveApiJsonFullActor(api: BaseApiDao, actorService: ActorUtil) extends Actor{
    override def receive: Receive = {
      case (isConcurrent: Boolean, startPage: Int) =>
        var currentPage = startPage
        if(isConcurrent)
          while(!api.finish){
            val mutiPageJsonActorRef = actorService.actorFactory.actorOf(Props(new MutiPageJsonActor))
            mutiPageJsonActorRef ! (currentPage, api)
            Thread.sleep(5000)
            currentPage += api.gradient
          }
        else
          while(!api.finish){
            api.saveJsonMutiPage(currentPage)
            currentPage += api.gradient
          }
        self ! "stop"
      case "stop" =>
        context.stop(self)
    }
  }

  class MutiPageJsonActor extends Actor{
    override def receive: Receive = {
      case (currentPage: Int, api: BaseApiDao) =>
        api.saveJsonMutiPage(currentPage, self)
      case "stop" =>
        context.stop(self)
    }
  }

}

