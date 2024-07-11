package spark.util

import java.text.SimpleDateFormat
import java.util.Date

import spark.common.Constants
import spark.common.config.TableConfig
import spark.dao.hive.impl.HiveDaoImpl
import spark.dao.kudu.impl.KuduDaoImpl
import spark.dao.mysql.impl.MySqlDaoImpl
import spark.dao.oracle.impl.OracleDaoImpl
import spark.dao.sqlserver.impl.SqlServerDaoImpl
import spark.exception.impl.ExceptionHandlerImpl
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.Console.println
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/11/30
 * Time: 9:56
 * Created by: 聂嘉良
 */

/**
 * 按照data_config.xml的参数接表
 */
class TableImportUtil(spark: SparkSession) {

  private val logger: Logger = Logger.getLogger(getClass)

  /**
   * 获取增量条件，目前支持数据源为oracle、sqlserver、mysql，数据目标为hive、kudu的表
   * 若指定begin和finish，则按照增量字段属于 [begin, finish) 区间来取数据，
   * 若未指定begin和finish：
   *   若delta_mode = "T0"，则取数据区间为 [从目标表获取增量字段的最大值, 源表增量字段最大值]
   *   若delta_mode = "T1"，则取数据区间为 [昨天0点, 今天0点)
   * @param source_type oracle/sqlserver/mysql
   * @param source_delta_fields 增量字段
   * @param target_type 目标表类型，hive/kudu
   * @param target_db 目标表所在数据库
   * @param target_table_name 目标表名
   * @param target_delta_fields 目标表增量字段
   * @param delta_mode 增量模式, "T0"/"T1"
   * @param yesterdayTime 昨天时间
   * @param todayTime 今天时间
   * @param begin 开始时间
   * @param finish 结束时间
   * @return 返回增量条件
   */
  private def generateDeltaCondition(source_type: String,
                             source_delta_fields: Array[String],
                             target_type: String,
                             target_db: String,
                             target_table_name: String,
                             target_delta_fields: Array[String],
                             delta_mode: String,
                             yesterdayTime: String,
                             todayTime: String,
                             begin: String,
                             finish: String): String ={
    val delta_condition = new StringBuilder()

    if(begin.equals("")  || finish.equals("")){
      //未指定增量区间的情况
      delta_mode match {
        case "T0" =>
          source_delta_fields.zip(target_delta_fields).foreach{
            case (source_delta_field, target_delta_field) =>
              var lastMaxValue = target_type match {
                case "hive" =>
                  spark.table(s"$target_db.$target_table_name")
                    .where(col(target_delta_field).isNotNull)
                    .select(date_format(max(col(target_delta_field)), "yyyy-MM-dd hh:mm:ss"))
                    .first.getAs[String](0)
                case "kudu" =>
                  new KuduDaoImpl(spark).read(Constants.KUDU_MASTER, target_db, target_table_name)
                    .where(col(target_delta_field).isNotNull)
                    .select(date_format(max(col(target_delta_field)), "yyyy-MM-dd hh:mm:ss"))
                    .first.getAs[String](0)
              }
              //判断日期格式，若结尾带.0，则
              //            if (lastMaxValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+"))
              //              lastMaxValue = lastMaxValue.substring(0,19)
              logger.info(lastMaxValue)
              source_type match {
                case "oracle" =>
                  delta_condition.append(s" and $source_delta_field > to_date('$lastMaxValue', 'yyyy-mm-dd hh24:mi:ss')")
                case _ =>
                  delta_condition.append(s" and $source_delta_field > '$lastMaxValue'")
              }
          }
          delta_condition.substring(4)
        case "T1" =>
          source_delta_fields.foreach(source_delta_field => {
            source_type match {
              case "oracle" =>
                delta_condition
                  .append(s" and $source_delta_field >= to_date('$yesterdayTime', 'yyyy-mm-dd hh24:mi:ss')" +
                    s" and $source_delta_field < to_date('$todayTime', 'yyyy-mm-dd hh24:mi:ss')")
              case _ =>
                delta_condition
                  .append(s" and $source_delta_field >= '$yesterdayTime' and $source_delta_field < '$todayTime''")
            }
          })
          delta_condition.substring(4)
      }
    }
    else{
      //指定增量区间的情况
      source_delta_fields.foreach(source_delta_field => {
        source_type match {
          case "oracle" =>
            delta_condition
              .append(s" and $source_delta_field >= to_date('$begin', 'yyyy-mm-dd hh24:mi:ss')" +
                s" and $source_delta_field < to_date('$finish', 'yyyy-mm-dd hh24:mi:ss')")
          case _ =>
            delta_condition
              .append(s" and $source_delta_field >= '$begin' and $source_delta_field < '$finish''")
        }
      })
      delta_condition.substring(4)
    }
  }

  /**
   * 获取数据，目前支持oracle、sqlserver、mysql
   * @param source_type oracle/sqlserver/mysql
   * @param source_jdbc_url jdbc url
   * @param source_jdbc_user jdbc user
   * @param source_jdbc_password jdbc password
   * @param source_table_name 数据源表名
   * @param delta_condition 增量条件，为""时代表全量取数据
   * @return 返回拿到的数据
   */
  private def fetchJdbcData(source_type: String,
                    source_jdbc_url: String,
                    source_jdbc_user: String,
                    source_jdbc_password: String,
                    source_table_name: String,
                    delta_condition: String = ""): DataFrame = {

    //delta_condition=""代表全量，否则按其增量
    val df = delta_condition match {
      case "" =>
        source_type match {
          case "oracle" =>
            new OracleDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, source_table_name)
          case "sqlserver" =>
            new SqlServerDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, source_table_name)
          case "mysql" =>
            new MySqlDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, source_table_name)
        }
      case condition: String =>
        source_type match {
          case "oracle" =>
            val sql = s"select * from $source_table_name where $condition"
            logger.info(sql)
            new OracleDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, s"($sql)")
          case "sqlserver" =>
            val sql = s"select * from $source_table_name where $condition"
            logger.info(sql)
            new SqlServerDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, s"($sql) t")
          case "mysql" =>
            val sql = s"select * from $source_table_name where $condition"
            logger.info(sql)
            new MySqlDaoImpl(spark).read(source_jdbc_url, source_jdbc_user, source_jdbc_password, s"($sql) t")
        }
    }
    df
  }

  /**
   * 保存数据，目前支持hive、kudu
   * @param target_type 目标表类型，hive/kudu
   * @param target_db 目标表所在数据库
   * @param target_table_name 目标表名
   * @param target_partition_field 目标表的分区字段（hive额表）
   * @param update_type full/deata/fullpart/deltapart
   * @param data 要存的数据
   */
  private def saveData(target_type: String,
               target_db: String,
               target_table_name: String,
               target_partition_field: String,
               update_type: String,
               data: DataFrame): Unit ={

    target_type match {
      case "hive" =>
        update_type match {
          case "full" =>
            new HiveDaoImpl(spark).write(target_db, target_table_name, data, SaveMode.Overwrite)
          case "delta" =>
            new HiveDaoImpl(spark).write(target_db, target_table_name,
              DataFrameUtil.cleanDataFrame(data, spark.table(s"$target_db.$target_table_name").schema, spark),
              SaveMode.Append)
          case "fullpart" =>

            target_partition_field.split("-") match{
              case Array(partition_col, "year") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  data
                    .withColumn("year", year(col(partition_col))),
                  SaveMode.Overwrite,
                  Array("year")
                )
              case Array(partition_col, "month") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  data
                    .withColumn("year", year(col(partition_col)))
                    .withColumn("month", month(col(partition_col))),
                  SaveMode.Overwrite,
                  Array("year", "month"))
              case Array(partition_col, "day") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  data
                    .withColumn("year", year(col(partition_col)))
                    .withColumn("month", month(col(partition_col)))
                    .withColumn("day", dayofmonth(col(partition_col))),
                  SaveMode.Overwrite,
                  Array("year", "month", "day"))
            }
          case "deltapart" =>
            target_partition_field.split("-") match{
              case Array(partition_col, "year") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  DataFrameUtil.cleanDataFrame(data
                    .withColumn("year", year(col(partition_col))),
                    spark.table(s"$target_db.$target_table_name").schema, spark),
                  SaveMode.Append,
                  Array("year")
                )
              case Array(partition_col, "month") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  DataFrameUtil.cleanDataFrame(data
                    .withColumn("year", year(col(partition_col)))
                    .withColumn("month", month(col(partition_col))),
                    spark.table(s"$target_db.$target_table_name").schema, spark),
                  SaveMode.Append,
                  Array("year", "month"))
              case Array(partition_col, "day") =>
                new HiveDaoImpl(spark).write(target_db,
                  target_table_name,
                  DataFrameUtil.cleanDataFrame(data
                    .withColumn("year", year(col(partition_col)))
                    .withColumn("month", month(col(partition_col)))
                    .withColumn("day", dayofmonth(col(partition_col))),
                    spark.table(s"$target_db.$target_table_name").schema, spark),
                  SaveMode.Append,
                  Array("year", "month", "day"))
            }
        }
      case "kudu" =>
        update_type match {
          case "full" =>
            new KuduDaoImpl(spark).write(target_db, target_table_name, data, "overwrite")
          case "delta" =>
            val kuduDao = new KuduDaoImpl(spark)
            kuduDao.write(target_db, target_table_name,
              DataFrameUtil.cleanDataFrame(data, kuduDao.read(Constants.KUDU_MASTER, target_db, target_table_name).schema, spark),
              "append")
        }
    }
  }

  /**
   * 对于每个表的逻辑处理
   * @param source_type oracle/sqlserver/mysql，数据源类型
   * @param source_jdbc_url jdbc url，不需要带用户名密码
   * @param source_jdbc_user jdbc用户名
   * @param source_jdbc_password jdbc密码
   * @param source_table_name 目标表名
   * @param comment_info 源表元数据信息
   * @param tb_column 元数据--表名
   * @param col_column 元数据--列名
   * @param comment_column 元数据--注释
   * @param source_delta_field 源表增量字段
   * @param target_type hive/kudu，目标表类型
   * @param target_db 目标表库名
   * @param target_table_name 目标表表名
   * @param target_partition_field 目标表分区字段
   * @param target_delta_field 目标表增量字段
   * @param begin 增量字段起始值，默认为""，若为""，表示不规定，按照配置文件更新增量数据
   * @param finish 增量字段结束值，默认为""，若为""，表示不规定，按照配置文件更新增量数据
   */
  private def eachTableLogic(source_type: String,
                     source_jdbc_url: String,
                     source_jdbc_user: String,
                     source_jdbc_password: String,
                     source_table_name: String,
                     comment_info: DataFrame,
                     tb_column: String,
                     col_column: String,
                     comment_column: String,
                     source_delta_field: String,
                     target_type: String,
                     target_db: String,
                     target_table_name: String,
                     target_partition_field: String,
                     target_delta_field: String,
                     yesterdayTime: String,
                     todayTime: String,
                     begin: String,
                     finish: String): Unit = {


    var df: DataFrame = null

    if (TableConfig.update_type == "full" || TableConfig.update_type == "fullpart") {
      df = fetchJdbcData(source_type,
        source_jdbc_url,
        source_jdbc_user,
        source_jdbc_password,
        source_table_name)

      /*
      如果存在comment列，则给dataframe加comment
      */
      if(comment_info.schema.map(_.name).contains(comment_column)){

        val colName_comments_map =
          comment_info.where(lower(col(tb_column)) === source_table_name.toLowerCase)
            .select(col(col_column), trim(regexp_replace(col(comment_column), "\\r|\\n|\\t", ","))
              .as(comment_column))
            .na
            .fill("", Array(comment_column))
            .collect()
            .map(row => (row.getString(0), row.getString(1)))
            .toMap

        val schema_commented = df.schema.map(s => {
          s.withComment(colName_comments_map(s.name))
        })

        df = spark.createDataFrame(df.rdd, StructType(schema_commented))

      }
    }
    if ((TableConfig.update_type == "delta") || (TableConfig.update_type == "deltapart")) {
      df = fetchJdbcData(source_type,
        source_jdbc_url,
        source_jdbc_user,
        source_jdbc_password,
        source_table_name,
        generateDeltaCondition(
          source_type,
          source_delta_field.split("-"),
          target_type,
          target_db,
          target_table_name,
          target_delta_field.split("-"),
          TableConfig.delta_mode,
          yesterdayTime,
          todayTime,
          begin,
          finish))
    }
    saveData(target_type,
      target_db,
      target_table_name,
      target_partition_field,
      TableConfig.update_type,
      df)
  }

  /**
   * 按照data_config.xml的参数接表，若不指定begin和finish，则按照配置的 delta_mode 参数接表
   * delta_mode = "T0"，接入数据区间为：[上次接入的最大数据, 现在]
   * delta_mode = "T1"，接入数据区间为：[昨天0点, 今天0点)
   * @param begin 指定开始时间，格式 yyyy-MM-dd hh:mm:ss
   * @param finish 指定结束时间，格式 yyyy-MM-dd hh:mm:ss
   */
  def accessTables(begin: String = "", finish: String = ""): Unit = {

    TableConfig.tableArgs.foreach(logger.info)

    /*
    获取元数据相关信息
    oracle的元数据信息记录在all_col_comments表中
    SqlServer和MySQL的元数据记录在INFORMATION_SCHEMA.COLUMNS表中
     */
    val (metadata_table, tb_table, db_column, tb_column, col_column, comment_column) =
      TableConfig.source_type match {
        case "oracle" => ("all_col_comments",
          "all_tables",
          "owner",
          "TABLE_NAME",
          "COLUMN_NAME",
          "COMMENTS")
        case _ => ("INFORMATION_SCHEMA.COLUMNS",
          "INFORMATION_SCHEMA.TABLES",
          "TABLE_SCHEMA",
          "TABLE_NAME",
          "COLUMN_NAME",
          "COLUMN_COMMENT")
      }

    val comment_info = fetchJdbcData(
      TableConfig.source_type,
      TableConfig.source_jdbc_url,
      TableConfig.source_jdbc_user,
      TableConfig.source_jdbc_password,
      metadata_table
    ).where(col(db_column).equalTo(TableConfig.source_db))
    //    comment_info.show(false)

    //初始化actor并发服务
    val actorUtil = new ActorUtil

    //初始化错误信息列表
    val errList = new ListBuffer[String]

    //预先规定昨天时间和今天时间，因为是静态变量，不能放到下面定义
    val yesterdayTime = DateUtil.getYesterdayDate + " 00:00:00"
    val todayTime = DateUtil.getTodayDate + " 00:00:00"

    //并发接入每个表并收集异常
    TableConfig.tableArgs.foreach {
      case (source_type,
      source_jdbc_url,
      source_jdbc_user,
      source_jdbc_password,
      source_table_name: String,
      source_delta_field: String,
      target_type,
      target_db,
      target_table_name: String,
      target_delta_field: String,
      target_partition_field) =>

        actorUtil.doSthAndCollectException(
          eachTableLogic(source_type, source_jdbc_url, source_jdbc_user, source_jdbc_password, source_table_name,
            comment_info, tb_column, col_column, comment_column, source_delta_field,
            target_type, target_db, target_table_name, target_partition_field, target_delta_field, yesterdayTime, todayTime,
            begin, finish),
          s"$target_db.$target_table_name", errList)
    }

    //关闭并发服务并等待执行完成
    actorUtil.close

    //发邮件逻辑
    if(errList.isEmpty){
      new MailUtil(Constants.MAIL_URL).send(Constants.MAIL_USER, Constants.MAIL_PASSWORD, Constants.MAIL_TO, s"${spark.sparkContext.appName} ${TableConfig.target_type} ${TableConfig.update_type} 数据接入结果", "接入成功！", "", "")
    }else{
      val errs = new StringBuffer()
      errList.foreach(err => errs.append(err + "\n"))
      new MailUtil(Constants.MAIL_URL).send(Constants.MAIL_USER, Constants.MAIL_PASSWORD, Constants.MAIL_TO, s"${spark.sparkContext.appName} ${TableConfig.target_type} ${TableConfig.update_type} 数据接入结果", s"接入发生异常，异常表为：$errs", "", "")
    }
  }

  /**
   * auth:linxiongcheng
   * @param tableName
   * @param updateStartTime
   * @param status
   * @param info
   * @param updateEndTime
   */
  def updateTable(tableName:String,updateStartTime:String,status:String,info:String,updateEndTime:String): Unit = {
    //      spark.sql("insert into gree_screendisplay_hr.hr_table_update_record values ('"+tableName+"','"+updateStartTime+"','"+status+"','"+info+"','"+updateEndTime+"')")
    spark.sql(s"insert into gree_screendisplay_hr.hr_table_update_record values ('$tableName','$updateStartTime','$status','$info','$updateEndTime')")
    //      spark.sql("invalidate metadata gree_screendisplay_hr.hr_table_update_record")
    new ImpalaUtil(Constants.IMPALA_URL).invalidateTable("gree_screendisplay_hr.hr_table_update_record")
  }

  /**
   * 当前日期
   * @return
   */
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }

  /**
   * auth:linxiongcheng
   * 写入数据并且更新记录表
   * @param writeFunction  函数
   * @param tableName 表名
   */
  def updateRecordTable(writeFunction: => Unit,tableName:String):Unit={
    var updateStartTime = ""
    var updateEndTime = ""
    Try{
      updateStartTime = NowDate()
      //外部传进来的函数，用来写入数据
      writeFunction
      updateEndTime= NowDate()
    }    match {
      case Success(_) =>
        updateTable(tableName,updateStartTime,"1","update success",updateEndTime)
        println(tableName+"更新成功")
      case Failure(e) =>
        //更新监控表(hr_table_update_record)状态
        updateTable(tableName,updateStartTime,"0",e.toString,updateEndTime)
        new ExceptionHandlerImpl(e).onException(s"Hive写入 $tableName 发生异常!")
        println(tableName+"更新失败")
    }
  }
}
