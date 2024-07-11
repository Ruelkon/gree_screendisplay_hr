package spark.dao.mysql

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.util.DateUtil

/**
 * Author: 260371
 * Date: 2021/8/13
 * Time: 13:52
 * Created by: 聂嘉良
 */
abstract class BaseMySqlDao {

  var spark: SparkSession

  val logger: Logger = Logger.getLogger(getClass)

  def read(url: String, userName: String, password: String, table: String): DataFrame = {

    spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", url)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", table)
      .load()
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

    df.write.mode(SaveMode.Append).option("driver","com.mysql.jdbc.Driver").jdbc(url, table, connectionProperties)
    println("【"+table+"】->MySQL at "+DateUtil.getTimeNow+"保存完成")
  }
}
