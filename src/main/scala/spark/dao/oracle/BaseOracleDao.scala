package spark.dao.oracle

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.util.DateUtil

import scala.collection.mutable.ArrayBuffer

/**
 * Author: 260371
 * Date: 2021/8/5
 * Time: 12:35
 * Created by: 聂嘉良
 */
abstract class BaseOracleDao {

  val logger: Logger = Logger.getLogger(getClass)

  var spark: SparkSession

  def read(url: String, userName: String, password: String, table: String): DataFrame ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", userName)
    connectionProperties.setProperty("password", password)
    connectionProperties.setProperty("characterEncoding", "utf8")
    val arr = ArrayBuffer[Int]()
    val partition_count = 10
    for(i <- 0 until partition_count){
      arr.append(i)
    }
    val predicates = arr.map(i => s"mod(row_id,$partition_count) = $i").toArray

    spark.read
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .jdbc(url, s"(select a.*, rownum row_id from $table a) b", predicates, connectionProperties)
      .drop("row_id")
  }

  def write(url: String, userName: String, pwd: String, table:String, df:DataFrame, mode:String): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", userName)
    connectionProperties.setProperty("password", pwd)
    connectionProperties.setProperty("characterEncoding", "utf8")

    val conn = DriverManager.getConnection(url, userName, pwd)
    if(mode == "overwrite"){
      val updateStmt = conn.createStatement()
      updateStmt.executeUpdate("truncate table "+table)
    }

    df.write
      .mode(SaveMode.Append)
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .jdbc(url, table, connectionProperties)
    logger.info("【"+table+"】->MySQL at "+DateUtil.getTimeNow+"保存完成")
  }
}
