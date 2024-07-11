package spark.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.exception.impl.ExceptionHandlerImpl
import spark.log.impl.LogHandlerImpl

import scala.util.{Failure, Success, Try}

/**
 * Spark应用基类
 * Author: 260371
 * Date: 2021/11/5
 * Time: 15:36
 * Created by: 聂嘉良
 */
abstract class BaseSparkApp extends AppTrait {
  protected final var spark: SparkSession = _

  /**
   * 启动应用，项目的总逻辑
   */
  final def startApp(): Unit = {

    //该处提供了简单的异常处理逻辑，也可以改成自己的
    Try[Unit]{

      onInit()

      createSession()

      onRun()

      onStop()

      onDestroyed()

    } match {
      case Success(_) =>
      case Failure(e) =>

        e.printStackTrace()

        new ExceptionHandlerImpl(e).onException(s"Spark程序 ${spark.sparkContext.appName} 运行发生异常！")

        new LogHandlerImpl(spark).handleLog()
    }

  }

  /**
   * 手动停止应用
   */
  final def stopApp(): Unit = {
    onStop()
  }

  /**
   * 创建SparkSession
   */
  def createSession(): Unit = {
    spark = SparkSession.builder()
      //要发布到服务器时需要注释
//      .master("local[*]")
      .config(getConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Spark应用配置，可根据自己的需要添加或删除
   */
  protected def getConf: SparkConf = {
    new SparkConf()
      .set("spark.port.maxRetries", "128")
      .set("spark.debug.maxToStringFields", "400")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.max.dynamic.partitions", "30000")
//      .set("spark.driver.memory","571859200")
//      .set("spark.testing.memory","2147480000")
      .set("spark.sql.broadcastTimeout", "36000")

  }

  /**
   * 初始化，应用运行前
   */
  override protected def onInit(): Unit = {}

  /**
   * 在此处调用service层代码
   */
  override def onRun(): Unit

  /**
   * 应用结束
   */
  override protected def onStop(): Unit = {

    //处理程序运行日志
    new LogHandlerImpl(spark).handleLog()

    //停止session
    if(spark != null) spark.stop()
  }

  /**
   * 应用销毁后调用
   */
  override protected def onDestroyed(): Unit = {}

}
