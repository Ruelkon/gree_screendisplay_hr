package spark.log

import java.io.{FileInputStream, FileReader, InputStreamReader}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import spark.common.Constants
import spark.exception.impl.ExceptionHandlerImpl

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/11/24
 * Time: 14:44
 * Created by: 聂嘉良
 */
abstract class BaseLogHandler extends LogHandlerTrait {

  val logger: Logger = Logger.getLogger(getClass)

  var spark: SparkSession

  //初始化props
  protected var props: Properties = new Properties()

  //初始化logs
  protected var logs: ListBuffer[FileReader] = new ListBuffer[FileReader]

  /**
   * 读取properties文件
   */
  override protected def loadProperties(): Unit = {


    val propsPath = Constants.LOG4J_PATH

    val propsReader = new InputStreamReader(new FileInputStream(propsPath), "UTF-8")

    //加载log4j.properties
    props.load(propsReader)
  }

  /**
   * 将log4j.properties中配置的日志路径解析成绝对路径
   * @param logPath log4j.properties中配置的日志路径，可能含有环境变量
   * @return
   */
  private def parsePathInProps(logPath: String): String ={
    val pathArr = logPath.split("/")

    //日志文件名
    val logName = pathArr.last

    val dirs = pathArr.slice(0, pathArr.length - 1)

    val absoultPath = dirs.map(dir => {
      val matcher = "^\\$\\{(\\S+)}$".r.pattern.matcher(dir)
      if (matcher.find) System.getProperty(matcher.group(1))
      else dir
    }).mkString("/") + "/" + logName

    absoultPath
  }

  /**
   * 日志处理实现
   */
  override def handleLog(): Unit = {

    //加载log4j.properties
    loadProperties()

    //获取日志的所有输出格式
    val targets = props.getProperty("log4j.rootLogger").split(",").tail

    //将所有文件格式日志的配置路径上传到hdfs上
    //上传到的hdfs默认路径为：/user/当前spark用户名(你的邮箱号)/logs/你的spark应用名/当前spark应用的ApplicationId/日志原文件名
    targets.foreach(target => {

      //log4j.properties中配置的日志路径
      val logPath = props.getProperty(s"log4j.appender.$target.File")

      if (logPath != null) {
        val fs = FileSystem.get(new Configuration())
        Try(fs.copyFromLocalFile(new Path(parsePathInProps(logPath)),
          new Path(s"hdfs://nameservice1/user/${spark.sparkContext.sparkUser}/logs/${spark.sparkContext.appName}/${spark.sparkContext.applicationId}/${logPath.split("/").last}")))
        match {
          case Success(value) =>
            logger.info("日志上传完成")
          case Failure(exception) =>
            exception.printStackTrace()
            new ExceptionHandlerImpl(exception).onException("Spark日志上传异常")
        }
      }
    })
  }
}
