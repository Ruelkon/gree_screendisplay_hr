package spark.service

/**
 * 将各个表中需要的数据联合成一张大表，插入hive库中，可以按天插入或者按时间段插入
 * 对应的jar包是spark-bigtable50.jar
 */

import java.io.{File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, when}
import spark.common.Constants
import spark.common.Constants.{os, parseValue}
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{KerberosUtil, TableImportUtil}

import scala.collection.mutable.ListBuffer

class insertIntoHrBigTable(spark: SparkSession) {
  def logic(): Unit = {
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    }

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    val actorUtil = new ActorUtil
//    val errList = new ListBuffer[String]
//    actorUtil.doSthAndCollectException(
//      {
        //按时间段更新数据,需要设置起始日期参数，生效参数是startDate和endData，指定日期参数无效
//        joinBigTableToTimeline()
        //按天更新，需要设置指定日期参数,生效参数是getHrDateAccordingToDate.oneDay，时间段参数无效
//        joinBigTableToCurrentTime()

        //更新50天的数据
        new TableImportUtil(spark).updateRecordTable(
        joinBigTableToFiftyDay()
          ,"hr_bigtable_dayrecord"
        )

//      }, "ex1", errList)
//
//    val logger: Logger = Logger.getLogger(getClass)
//    logger.error(errList)
//    actorUtil.close

    //写入gree_screendisplay_hr中的视图表,电商部门需要用到
//    spark.sql("insert overwrite table gree_screendisplay_hr.hr_bigtable_dayrecord_view_computer select empname,empnum,deptnamel1,deptnamel2,systemcategory,value2,`date` from gree_screendisplay_hr.hr_bigtable_dayrecord")
//    new ImpalaUtil(Constants.IMPALA_URL).invalidateTable(s"gree_screendisplay_hr.hr_bigtable_dayrecord_view")
  }

  //按照当天日期更新大表
  def joinBigTableToCurrentTime(): Unit ={
    val nowDate = LocalDate.now()
    val admin = spark.table("gree_screendisplay_hr.ods_org_admin").as("admin")
    val orgfunction = spark.table("gree_screendisplay_hr.ods_org_orgfunction").as("orgfunction")
    val reserveitemfirst = spark.table("gree_screendisplay_hr.ods_org_reserveitemfirst").as("reserveitemfirst")
    val sys_company = admin.repartition(10)
      .join(orgfunction,admin("forgfunctionid")===orgfunction("fid"),"left")
      .join(reserveitemfirst,admin("freserveitemfirst")===reserveitemfirst("fid"),"left")
      .select(col("admin.fnumber"),col("admin.forgfunctionid"),col("admin.flongnumber"),col("admin.fsortcode"),
        col("admin.freserveitemfirst"),
        col("orgfunction.fname_l2").as("systemcategory"),
        col("orgfunction.fnumber").as("systemnumber"),
        col("reserveitemfirst.fname_l2").as("companycategory"),
        col("reserveitemfirst.fnumber").as("companynumber"))
      .withColumn("systemcategory",when(col("systemcategory")===null,"其他").otherwise(col("systemcategory")))
      .withColumn("companycategory",when(col("companycategory")===null,"其他").otherwise(col("companycategory"))).as("sysCom") //注意这里的as要放在withcolumn后面，不然会报无法解析sysCom相关的错误！
    sys_company.cache()
//    println("=========sys_com数量："+sys_company.count())
    sys_company.limit(10).show()

    //读取通用配置
    val app_config = new Properties()
    app_config.load(
      new InputStreamReader(
        if(os.indexOf("linux") >= 0)
          new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
        else
          getClass.getClassLoader.getResourceAsStream("app_config.properties")
        , "UTF-8"))
    //从配置文件中读取指定日期
    var date: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.oneDay"))
    //如果日期为空，则从json获取昨天的日期插入hive
    if(date == "null"){
      date = "date='"+nowDate.plusDays(-1).toString+"T00:00:00'"
//      date = "date>="+nowDate.plusDays(-50).toString+"T00:00:00 and date<="+nowDate.plusDays(-1).toString+"T00:00:00"
    }
    joinTable(date, sys_company)
  }

  //按照时间段更新大表，和上面方法重复代码太多，可以想象怎么优化
  def joinBigTableToTimeline(): Unit ={
    val admin = spark.table("gree_screendisplay_hr.ods_org_admin").as("admin")
    val orgfunction = spark.table("gree_screendisplay_hr.ods_org_orgfunction").as("orgfunction")
    val reserveitemfirst = spark.table("gree_screendisplay_hr.ods_org_reserveitemfirst").as("reserveitemfirst")
    val sys_company = admin.repartition(10)
      .join(orgfunction,admin("forgfunctionid")===orgfunction("fid"),"left")
      .join(reserveitemfirst,admin("freserveitemfirst")===reserveitemfirst("fid"),"left")
      .select(col("admin.fnumber"),col("admin.forgfunctionid"),col("admin.flongnumber"),col("admin.fsortcode"),
        col("admin.freserveitemfirst"),
        col("orgfunction.fname_l2").as("systemcategory"),
        col("orgfunction.fnumber").as("systemnumber"),
        col("reserveitemfirst.fname_l2").as("companycategory"),
        col("reserveitemfirst.fnumber").as("companynumber"))
      .withColumn("systemcategory",when(col("systemcategory")===null,"其他").otherwise(col("systemcategory")))
      .withColumn("companycategory",when(col("companycategory")===null,"其他").otherwise(col("companycategory"))).as("sysCom")
    sys_company.cache()
//    println("=========sys_com数量："+sys_company.count())
    sys_company.limit(10).show()

    //读取通用配置
    val app_config = new Properties()
    app_config.load(
      new InputStreamReader(
        if(os.indexOf("linux") >= 0)
          new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
        else
          getClass.getClassLoader.getResourceAsStream("app_config.properties")
        , "UTF-8"))
    //从配置文件中读取时间段
    val startDate: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.startDate"))
    val endDate: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.endDate"))
    var actualdate = "date>='"+startDate+"T00:00:00' and date<='"+endDate+"T00:00:00'"
//    for(date <- getBetweenDates(startDate,endDate)){
//      var actualdate = "date='"+date+"T00:00:00'"
      joinTable(actualdate, sys_company)
//    }
  }

  //更新当天及前50天的大表
  def joinBigTableToFiftyDay(): Unit ={
    val nowDate = LocalDate.now()
    val admin = spark.table("gree_screendisplay_hr.ods_org_admin").as("admin")
    val orgfunction = spark.table("gree_screendisplay_hr.ods_org_orgfunction").as("orgfunction")
    val reserveitemfirst = spark.table("gree_screendisplay_hr.ods_org_reserveitemfirst").as("reserveitemfirst")
    val sys_company = admin.repartition(10)
      .join(orgfunction,admin("forgfunctionid")===orgfunction("fid"),"left")
      .join(reserveitemfirst,admin("freserveitemfirst")===reserveitemfirst("fid"),"left")
      .select(
        col("admin.fnumber"),
//        col("admin.forgfunctionid"),
        col("admin.flongnumber"),
        col("admin.fsortcode"),
        col("admin.fdisplayname_l2"),
//        col("admin.freserveitemfirst"),
        col("orgfunction.fname_l2").as("systemcategory"),
        col("orgfunction.fnumber").as("systemnumber"),
        col("reserveitemfirst.fname_l2").as("companycategory"),
        col("reserveitemfirst.fnumber").as("companynumber"))
      .withColumn("systemcategory",when(col("systemcategory")===null,"其他").otherwise(col("systemcategory")))
      .withColumn("companycategory",when(col("companycategory")===null,"其他").otherwise(col("companycategory"))).as("sysCom")
    sys_company.cache()
//    println("=========sys_com数量："+sys_company.count())
    sys_company.limit(10).show()

    val startDate: String = nowDate.plusDays(-50).toString

    val endDate: String = nowDate.plusDays(-1).toString
    val tableName = "hr_bigtable_dayrecord"
    var actualdate = "date>='"+startDate+"T00:00:00' and date<='"+endDate+"T00:00:00'"
    for(date <- getBetweenDates(startDate,endDate)){
      //循环删除分区，因为alter语句在spark3.0.0之前的版本不能使用><来删除分区段
      println("当前分区日期："+date);
      spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition (date='"+date+"T00:00:00')")
    }
        joinTable(actualdate, sys_company)
//    joinTable(date, sys_company)
  }

  /**
   * 拼成大表
   * @param dateFilterContition 针对date的filter条件
   * @param sys_company
   */
  def joinTable(dateFilterContition:String,sys_company:DataFrame): Unit ={
    val sparkSession = new HiveDaoImpl(spark)
    val admin = spark.table("gree_screendisplay_hr.ods_org_admin").as("admin")
    val emp = spark.table("gree_screendisplay_hr.hr_emp_info").filter(dateFilterContition).withColumn("isleave",
      //        when(emp("entrydate") <= attendance("date") and emp("leavedate").isNull,"false")
      //          when(emp("entrydate") <= attendance("date") and (emp("leavedate") > attendance("date")),"false")
      //          when(emp("entrydate") >= attendance("date") or (emp("leavedate") <= attendance("date")),"true")
      //          otherwise(emp("isleave"))
      when(col("entrydate") <= col("date") and col("leavedate").isNull,false)
        when(col("entrydate")<=col("date") and(col("leavedate")>col("date")),false)
        when(col("entrydate")>=col("date") or(col("leavedate")<=col("date")),true)
        otherwise(col("isleave"))
    ).as("emp")
//    emp.limit(10).show()
//    println("emp输出")
    //    val record = spark.table("gree_screendisplay_hr.hr_card_record").as("record")
    //这里的dateFilterContition.replace("date","signdate")是用来替换不同表的不同分区字段，有的是date，有的是signdate，为了传参方便，用这种方式
    val record = spark.table("gree_screendisplay_hr.hr_card_record").filter(dateFilterContition.replace("date","signdate")).as("record")
//    record.limit(10).show()
//    println("record输出")
    record.cache()
    val dayWork = spark.table("gree_screendisplay_hr.hr_day_attendance_work").as("dayWork")
//    dayWork.limit(10).show()
//    println("daywork输出")
    val dept = spark.table("gree_screendisplay_hr.hr_dept_info")
      .withColumn("deptnamel2",when(col("deptnamel2")==="",null).otherwise(col("deptnamel2")))
      .withColumn("deptnamel3",when(col("deptnamel3")==="",null).otherwise(col("deptnamel3")))
      .withColumn("deptnamel4",when(col("deptnamel4")==="",null).otherwise(col("deptnamel4")))
      .withColumn("deptnamel5",when(col("deptnamel5")==="",null).otherwise(col("deptnamel5"))).as("dept")
    //    val attendance = spark.table("gree_screendisplay_hr.hr_day_attendance").as("attendance")
    dept.limit(10).show()
    println("dept输出")
    val attendance = spark.table("gree_screendisplay_hr.hr_day_attendance").filter(dateFilterContition).as("attendance")
//    attendance.limit(10).show()
    attendance.cache()
    var baseTable = attendance.repartition(10)
      .join(record,attendance("empid")===record("empid")&&attendance("date")===record("signdate"),"left")
      .join(dayWork,record("shiftcode")===dayWork("shiftcode"),"left")
      .join(emp,emp("empid")===attendance("empid")&&emp("date")===attendance("date"),"left")
      .join(dept,emp("empdept")===dept("deptcode"),"left")
      .join(sys_company,dept("deptcode")===sys_company("fnumber"),"left")
      .filter("empnum is not null") //为了过滤当天新增加的人员（默认不考虑）

    baseTable = baseTable.select("emp.empnum","emp.emprank","emp.empname","emp.isleave","emp.atttype","record.shiftcode","dayWork.shiftname",
        "dept.deptcode","dept.deptnamel1","dept.deptnamel2","dept.deptnamel3","dept.deptnamel4","dept.deptnamel5",
        "sysCom.systemcategory","sysCom.systemnumber","sysCom.companycategory","sysCom.companynumber","sysCom.flongnumber","sysCom.fsortcode",
        "attendance.recordid","attendance.empid","attendance.updatetime","attendance.value1","attendance.value2","attendance.value3","attendance.value4","attendance.value5",
        "attendance.value6","attendance.value7","attendance.value8","attendance.value9","attendance.value10","attendance.value11","attendance.value12","attendance.value13",
        "attendance.value14","attendance.value15","attendance.value16","attendance.value17","attendance.value18","attendance.value19","attendance.value20","attendance.value21",
        "attendance.value22","attendance.value23","attendance.value24","attendance.value25","attendance.value26","attendance.value27","attendance.value28","attendance.value29",
        "attendance.value30","attendance.value31","attendance.value32","attendance.value33","attendance.value34","attendance.value35","attendance.value36","attendance.value37","attendance.value38",
      "attendance.value39","attendance.value40","attendance.value41","attendance.value42","attendance.value43","attendance.value44","attendance.value45","attendance.value46","attendance.value47",
      "attendance.value48","attendance.value49","attendance.value50","attendance.value51","attendance.value52","attendance.value53","attendance.value54","attendance.value55",
      "attendance.date")
    //找到fnumber为空的值，过滤掉admin相关的字段，后面再重新join之后重新添加对应列,注意要删掉join后新增加的字段，不然无法union
//    val updateDf = baseTable.where("fnumber is null")
//      .drop("systemcategory","systemnumber","flongnumber","fsortcode","companycategory","companynumber")
//      .join(admin,concat_ws("_",lit("格力电器"),baseTable("deptnamel1"),baseTable("deptnamel2"))===admin("fdisplayname_l2"),"left")
//      .drop("fnumber","fdisplayname_l2")
//    baseTable = baseTable.where("fnumber is not null").union(updateDf)
    println("baseTable输出")
//    .select(col("admin.fnumber"),col("admin.forgfunctionid"),col("admin.flongnumber"),col("admin.fsortcode"),
    //        col("admin.freserveitemfirst"),
    //        col("orgfunction.fname_l2").as("systemcategory"),
    //        col("orgfunction.fnumber").as("systemnumber"),
    //        col("reserveitemfirst.fname_l2").as("companycategory"),
    //        col("reserveitemfirst.fnumber").as("companynumber"))
    baseTable.cache()
//    println("=========baseTable数量："+baseTable.count())
    baseTable.limit(10).show()
    sparkSession.write("gree_screendisplay_hr", "hr_bigtable_dayrecord", baseTable, SaveMode.Append,Array("date"))

//    bigTable.repartition(10).write.mode(SaveMode.Append).format("hive").partitionBy("date").saveAsTable("gree_screendisplay_hr.hr_bigtable_dayrecord")
//    println("插入完成,数量为"+baseTable.count())
      }

  /**
   * 获取两个日期之间的日期
   * @param start 开始日期
   * @param end 结束日期
   * @return 日期集合
   */
  def getBetweenDates(start: String, end: String) = {
    val startData = new SimpleDateFormat("yyyy-MM-dd").parse(start); //定义起始日期
    val endData = new SimpleDateFormat("yyyy-MM-dd").parse(end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var buffer = new ListBuffer[String]
    buffer += dateFormat.format(startData.getTime())
    val tempStart = Calendar.getInstance()

    tempStart.setTime(startData)
    tempStart.add(Calendar.DAY_OF_YEAR, 1)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer += dateFormat.format(endData.getTime())
    buffer.toList
  }
}

