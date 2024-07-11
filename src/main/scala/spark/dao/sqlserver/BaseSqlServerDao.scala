package spark.dao.sqlserver

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import spark.exception.impl.ExceptionHandlerImpl
import spark.util.DateUtil

import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/8/5
 * Time: 12:34
 * Created by: 聂嘉良
 */
abstract class BaseSqlServerDao {

  val logger: Logger = Logger.getLogger(getClass)

  var spark: SparkSession

  def read(url: String, userName: String, password: String, table: String, partitionColumn: String = ""): DataFrame = {
    //获取数据量
    val row_count =
      Try[Int]{
        if(table.toLowerCase().contains("view"))
          1000000
        else
          spark.read.format("jdbc")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("url", url)
            .option("user", userName)
            .option("password", password)
            .option("dbtable", s"(SELECT A.NAME name,B.ROWS rows FROM sysobjects A JOIN sysindexes B ON A.id = B.id WHERE A.xtype = 'U' AND B.indid IN(0,1)) a")
            .load()
            .where(col("name").equalTo(table.substring(table.indexOf(".") + 1)))
            .select("rows").first().getInt(0)
      }.getOrElse(100)


    Try[DataFrame] {
      if (partitionColumn.equals("")) {
        spark.read.format("jdbc")
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .option("url", url)
          .option("user", userName)
          .option("password", password)
          .option("dbtable", table)
          .load()
      } else {
        spark.read.format("jdbc")
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .option("url", url)
          .option("user", userName)
          .option("password", password)
          .option("dbtable", table)
          .option("numPartitions", 200)
          .option("partitionColumn", partitionColumn)
          .option("lowerBound", 1)
          .option("upperBound", 10000)
          .load()
      }
    } match {
      case Success(df) =>
        df.repartition(if (row_count < 10000) 1 else if (row_count < 1000000) 10 else if(row_count < 10000000) 100 else if(row_count < 20000000) 500 else 1000)
      case Failure(e) =>
        new ExceptionHandlerImpl(e).onException("获取SqlServer表"+table+"发生异常!")
        null
    }
  }

  def write(url: String, userName: String, pwd: String, table:String, df:DataFrame, mode:String): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", userName)
    connectionProperties.setProperty("password", pwd)
    connectionProperties.setProperty("characterEncoding", "utf8")
    val conn = DriverManager.getConnection(url, userName, pwd)
    if (mode == "overwrite") {
      val updateStmt = conn.createStatement()
      updateStmt.executeUpdate("truncate table " + table)
    }
    df.write.mode(SaveMode.Append).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url, table, connectionProperties)
    logger.info("【" + table + "】->SqlServer at " + DateUtil.getTimeNow + "保存完成")
  }
}
