package spark.dao.hive

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import spark.common.Constants
import spark.util.{DateUtil, ImpalaUtil}

import scala.Console.println
import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/11/6
 * Time: 14:16
 * Created by: 聂嘉良
 */
abstract class BaseHiveDao{

  var spark: SparkSession

  val logger: Logger = Logger.getLogger(getClass)

  def deleteHdfs(path: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(path), conf)
    val delPath = new Path(path)

    logger.info(s"删除数据 $delPath "+fs.delete(delPath, true))
  }

  /**
   * 读Hive表
   * @param db Hive库名
   * @param table Hive表名
   * @return
   */
  def read(db: String, table: String): DataFrame = {
    spark.table(s"$db.$table")
  }

  /**
   * 写入Hive表
   * @param db Hive库名
   * @param table Hive表名
   * @param data 要写入的数据
   * @param mode Append/Overwrite
   * @param partition_by 分区字段，默认无分区字段
   */
  def write(db: String, table: String, data: DataFrame, mode: SaveMode, partition_by: Array[String] = Array()): Unit = {
    spark.sql(s"use $db")
//    Try{
      data match {
      case df if df.schema.isEmpty =>
        logger.info(s"表${table}无数据且schema为空，不予保存到$db")
      case _ =>
        mode match {
          case SaveMode.Overwrite =>
            val uri = s"${Constants.HDFS_URL}/user/hive/warehouse/$db.db/$table"
            deleteHdfs(uri)

            if(partition_by.isEmpty)
              data.write.mode(SaveMode.Overwrite).saveAsTable(table)
            else
              data.write.mode(SaveMode.Overwrite).partitionBy(partition_by:_*).saveAsTable(table)

          case SaveMode.Append =>
            val format = spark.sql(s"describe formatted $table")
              .where(col("col_name") === "Provider")
              .select("data_type")
              .head.getString(0)
              .trim.toLowerCase

            //使数据字段和结果表字段一致再存入
            val target_field_names = spark.table(s"$db.$table").schema.fieldNames
            if(partition_by.isEmpty)
              data.repartition(3).select(target_field_names.head, target_field_names.tail:_*)
                .write.mode(SaveMode.Append).format(format).saveAsTable(table)
            else
              data.repartition(3).select(target_field_names.head, target_field_names.tail:_*)
                .write.mode(SaveMode.Append).format(format).partitionBy(partition_by:_*).saveAsTable(table)
        }
        println("保存完成")
        logger.info("【" + db + "." + table + "】->Hive at " + DateUtil.getTimeNow + "保存完成")
        Try(new ImpalaUtil(Constants.IMPALA_URL).invalidateTable(s"$db.$table")) match {
          case Success(_) => logger.info(s"刷新表 $db.$table 成功")
            println("刷新表成功")
          case Failure(e) =>
            println("刷新表失败")
            logger.info(s"刷新表 $db.$table 失败")
            e.printStackTrace()
        }
    }
//  }
//    match {
//      case Success(_) =>
//      case Failure(e) =>
//        new ExceptionHandlerImpl(e).onException(s"Hive写入 $db.$table 发生异常!")
//    }
  }

}
