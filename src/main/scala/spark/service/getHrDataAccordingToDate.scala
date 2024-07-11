package spark.service

/**
 * 这个类是最终实现类，可以实现按照时间段存数据和按天存数据到hive库中，用来实现多个接口同时接入。
 * 对应的jar包是spark-update50AndBaseTble.jar
 */

import java.io.{File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{column, lit, upper}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.spark_project.guava.collect.ImmutableMap
import spark.common.Constants
import spark.common.Constants.{os, parseValue}
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{ActorUtil, HttpUtils, KerberosUtil, TableImportUtil}

import scala.Console.println
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class getHrDataAccordingToDate(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass)
  def logic(): Unit = {
    // 判断当前操作系统,如果是windows系统，则表示本地运行，需要Kerberos认证
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    }
    val start = System.currentTimeMillis()
    parallelInsert()
    val end = System.currentTimeMillis()
    println("花费了"+(end - start)+"ms")
    println("查询成功")
  }

  //并行写入
  def parallelInsert(): Unit ={
    val actorUtil = new ActorUtil
    val errList = new ListBuffer[String]
    actorUtil.doSthAndCollectException(
          {
    //组织架构信息列表：dept_info
        val https = "https://hrapi.gree.com/apiv2/attendance/deptinfolist"
            new TableImportUtil(spark).updateRecordTable(
            jsonSinkHiveAllDate(https,"hr_dept_info"),"hr_dept_info"
            )
          }, "ex1", errList)
    actorUtil.doSthAndCollectException(
      {
        //员工信息列表：emp_info
        val https = "https://hrapi.gree.com/apiv2/attendance/empinfolist"
        new TableImportUtil(spark).updateRecordTable(
        jsonSinkHiveAllToEmp(https,"hr_emp_info","date"),"hr_emp_info"
        )
      }, "ex2", errList)
    actorUtil.doSthAndCollectException(
      {
        //day_attendance_work
        val https = "https://hrapi.gree.com/apiv2/attendance/interface15"
        new TableImportUtil(spark).updateRecordTable(
        jsonSinkHiveAllDate(https,"hr_day_attendance_work"),"hr_day_attendance_work"
        )
      }, "ex3", errList)
    actorUtil.doSthAndCollectException(
      {
        //cardrecord表
        val https = "https://hrapi.gree.com/apiv2/attendance/cardrecordlist"
        //按时间段更新数据,需要设置起始日期参数
//        jsonSinkHive(https,"SignDate","hr_card_record","SignDate")
        //按天更新，需要设置指定日期参数
//        jsonSinkHiveOneDay(https,"SignDate","hr_card_record","SignDate")
        new TableImportUtil(spark).updateRecordTable(
//        jsonSinkHiveSeveralday(https,"SignDate","hr_card_record","signdate",32),"hr_card_record"
        jsonSinkHiveSeveralday(https,"hr_card_record","signdate",32),"hr_card_record"
        )
      }, "ex4", errList)
    actorUtil.doSthAndCollectException(
      {
        //day_attendance表
        val https = "https://hrapi.gree.com/apiv2/attendance/interface21"
        //按时间段更新数据,需要设置起始日期参数，生效参数是startDate和endData，指定日期参数无效
//        jsonSinkHive(https,"Date","hr_day_attendance","Date")
        //按天更新，需要设置指定日期参数,生效参数是getHrDateAccordingToDate.oneDay，时间段参数无效
//        jsonSinkHiveOneDay(https,"Date","hr_day_attendance","Date")
        new TableImportUtil(spark).updateRecordTable(
        jsonSinkHiveSeveralday(https,"hr_day_attendance","date",50),"hr_day_attendance"
        )
      }, "ex5", errList)
    println("输入成功")
    logger.error(errList)
    actorUtil.close
  }

  //根据索引和获取数量返回json字符串
  /**
   *
   * @param https
   * @param beginIndex
   * @param getCount
   * @param date 格式："Date:"2021-11-11""或者"SignDate:"2021-11-11""
   * @return
   */
  def getJson(https:String,beginIndex:Int=1,getCount:Int=1,date:String=""): JSONObject ={
    val httpUtil = new HttpUtils()
    val client: CloseableHttpClient = httpUtil.createSSLClientDefault()
    //从接口获取数据（全量？如何分批）
    var bodyString = ""
    if(date==""){
    bodyString = "{\"AppID\":\"4E74E0EA3E76479B89207B2F1E9A4329\",\"Secret\":\"1A6A4C58784C4942B4CD5106DCA74E26\",\"Begin\":"+beginIndex+",\"Count\":"+getCount+"}"
    }else{
    bodyString = "{\"AppID\":\"4E74E0EA3E76479B89207B2F1E9A4329\",\"Secret\":\"1A6A4C58784C4942B4CD5106DCA74E26\","+date+", \"Begin\":"+beginIndex+",\"Count\":"+getCount+"}"
    }
    val map = new util.HashMap[String, String]
    map.put("Content-Type", "application/json")
    //    println ("获取的json数据为："+httpUtil.post(https, map, bodyString))
    val itemJsonArray = JSON.parseObject(httpUtil.post(client,https, map, bodyString))
    itemJsonArray
  }

//  def getTime:String = {
//    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    val zoneId = ZoneId.systemDefault
//    dtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(new Date().getTime), zoneId))
//  }

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

  /**
   * 按照起始日期将数据分批插入到分区中
   * @param https
   * @param dateName json中时间字段的名称，每个表不一样
   * @param tableName
   */
  def jsonSinkHive(https:String,dateName:String,tableName:String,partitionName:String): Unit ={
    //读取通用配置
    val app_config = new Properties()
    app_config.load(
      new InputStreamReader(
        if(os.indexOf("linux") >= 0)
          new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
        else
          getClass.getClassLoader.getResourceAsStream("app_config.properties")
        , "UTF-8"))
    //从配置文件中读取起始日期
    val startDate: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.startDate"))
    val endDate: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.endDate"))

    val sparkSession = new HiveDaoImpl(spark)
    var getCount = 5000
    for(date <- getBetweenDates(startDate,endDate)) {
      println(date)
      //按天分批存到hive分区
      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https,date = "\"SignDate\":\"2021-01-01\"").getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
//      intDataframe.show()
      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
      var offset = 1
      var insert = true
      while(insert){
         var jsonObject =  getJson(https,offset,getCount,"\""+dateName+"\":\"" + date + "\"")
        //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
        if (jsonObject.getInteger("count")<getCount){
          insert = false
        }
//        有时候json串中突然没数据了，导致读不到数据出现异常，所以要判断一下
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0){
          bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }

  /**
   * 按天更新今天的分区
   * @param https
   * @param dateName json中时间字段的名称，每个表不一样
   * @param tableName
   */
  def jsonSinkHiveOneDay(https:String,dateName:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
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

    val sparkSession = new HiveDaoImpl(spark)
    var getCount = 5000
    //如果日期为空，则从json获取当前日期前一天插入hive,因为当天日期的数据不完全。
    if(date == "null"){
    date = nowDate.plusDays(-1).toString
    }
      println(date)
      //按天分批存到hive分区
      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
//      intDataframe.show()
      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
      var offset = 1
      var insert = true
      while(insert){
        var jsonObject =  getJson(https,offset,getCount,"\""+dateName+"\":\"" + date + "\"")
        //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
        if (jsonObject.getInteger("count")<getCount){
          insert = false
        }
//        spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0){
          bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
  }


  /**
   * 按照起始日期将数据分批插入到分区中
   * @param https
   * @param tableName
   * @param partitionName json中时间字段的名称，每个表不一样
   * @param severalDays  定义要更新数据的天数
   */
  def jsonSinkHiveSeveralday(https:String,tableName:String,partitionName:String,severalDays:Int): Unit ={
    val nowDate = LocalDate.now()
    //起始日期：当前系统时间的前若干天
    val startDate: String = nowDate.plusDays(-severalDays).toString
    //截止日期：当前系统时间的前1天
    val endDate: String = nowDate.plusDays(-1).toString

    val sparkSession = new HiveDaoImpl(spark)
    val getCount = 5000
    val intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
    for(date <- getBetweenDates(startDate,endDate)) {
      println(date)
      //按天分批存到hive分区
      //      intDataframe.show()
      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
      var offset = 1
      var insert = true
      while(insert){
        var jsonObject =  getJson(https,offset,getCount,"\""+partitionName+"\":\"" + date + "\"")
        //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
        if (jsonObject.getInteger("count")<getCount){
          insert = false
        }
        //        spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0){
          bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
      //数据清洗：bigDataframe包含shiftcode列的（同时几个表用该方法接入，不同的表字段不同，需要判断一下），将小写字母转成大写，并且去除后面的空格
      if(bigDataframe.schema.fieldNames.contains("shiftcode")){
      bigDataframe = bigDataframe.withColumn("shiftcode",upper(column("shiftcode")))
        //去除字符末尾的空格
        .na.replace("shiftcode",ImmutableMap.of("NORM ","NORM"))
      }

      spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }

//  /**
//   * 全量按批存hive，不分区
//   * @param https
//   * @param tableName
//   */
//  def jsonSinkHiveAll(https:String,tableName:String): Unit ={
//    val sparkSession = new HiveDaoImpl(spark)
//      //按offset存到hive
//    //从spark中读取表的条数，条数加1就是偏移(这种方式是每次调用方法自动更新数据):前提是接口中的数据顺序是不变的
//    var insertCount = spark.sql("select * from gree_screendisplay_hr."+tableName+"").count().toInt
//    println(tableName+"表当前数据量："+insertCount)
//      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
//      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
//      var offset = insertCount+1
//      var insert = true
//      while(insert){
//        var jsonObject =  getJson(https,offset,5000)
//        //如果剩下的数量少于5000，结束循环标记
//        if (jsonObject.getInteger("count")<5000){
//          insert = false
//        }
//        bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
//        offset = offset+5000
//      }
//    //todo 加一个每批的计数
//      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append)
//    }

  /**
   * 全量按批存hive，不分区,单独全量存入hive，不然按照索引存会出现重复数据。
   * @param https
   * @param tableName
   */
  def jsonSinkHiveAllDate(https:String,tableName:String): Unit ={
    val sparkSession = new HiveDaoImpl(spark)
    //清空表后重新插入
    spark.sql("truncate table gree_screendisplay_hr."+tableName+"")
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var offset = 1
    var insert = true
    while(insert){
      var jsonObject =  getJson(https,offset,5000)
      //如果剩下的数量少于5000，结束循环标记
      if (jsonObject.getInteger("count")<5000){
        insert = false
      }
      val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
      if(getDf.columns.size!=0){
        bigDataframe = bigDataframe.unionAll(getDf)
      }
      offset = offset+5000
    }
    //todo 加一个每批的计数
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append)
  }

  /**
   * 按天全量按批存hive，分区,针对empinfo表,
   * @param https
   * @param tableName
   */
  def jsonSinkHiveAllToEmp(https:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
    //获取当天的日期
    val date = nowDate.toString+"T00:00:00"
    val sparkSession = new HiveDaoImpl(spark)
    //按offset存到hive
    //存在当天分区，删除当天的分区，重新插入(更新)
        spark.sql("alter table gree_screendisplay_hr." + tableName + " drop if exists partition ("+partitionName+"='" + date + "')")
    var insertCount = spark.sql("select * from gree_screendisplay_hr."+tableName+" where `date`='"+date+"'").count().toInt
    println(tableName+"表当前数据量："+insertCount)
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date",lit(date))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var offset = insertCount+1
    var insert = true
    while(insert){
      var jsonObject =  getJson(https,offset,5000)
      //如果剩下的数量少于5000，结束循环标记
      if (jsonObject.getInteger("count")<5000){
        insert = false
      }
      //这里注意要多添加一列date
      val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date", lit(date))
      if(getDf.columns.size!=0){
        bigDataframe = bigDataframe.unionAll(getDf)
      }
      offset = offset+5000
    }
    //todo 加一个每批的计数
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
  }

  //  /**
  //   * 更新今天及前50天的分区
  //   * @param https
  //   * @param dateName json中时间字段的名称，每个表不一样
  //   * @param tableName
  //   */
  //  def jsonSinkHiveFiftyDay(https:String,dateName:String,tableName:String,partitionName:String): Unit ={
  //    val nowDate = LocalDate.now()
  //    //读取通用配置
  //    val app_config = new Properties()
  //    app_config.load(
  //      new InputStreamReader(
  //        if(os.indexOf("linux") >= 0)
  //          new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
  //        else
  //          getClass.getClassLoader.getResourceAsStream("app_config.properties")
  //        , "UTF-8"))
  //    //从配置文件中读取指定日期
  //    var date: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.oneDay"))
  //
  //    val sparkSession = new HiveDaoImpl(spark)
  //    var getCount = 5000
  //    //如果日期为空，则从json获取当前日期前一天到前50天的数据插入hive,因为当天日期的数据不完全。
  //    if(date == "null"){
  //      //拼接between and语句
  //      date = "BETWEEN"+nowDate.plusDays(-50).toString+"AND"+nowDate.plusDays(-1).toString+""
  //    }
  //    println(date)
  //    //按天分批存到hive分区
  //    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https,date = "\"SignDate\":\"2021-01-01\"").getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
  //    //      intDataframe.show()
  //    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
  //    var offset = 1
  //    var insert = true
  //    while(insert){
  //      var jsonObject =  getJson(https,offset,getCount,"\""+dateName+"\":\"" + date + "\"")
  //      //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
  //      if (jsonObject.getInteger("count")<getCount){
  //        insert = false
  //      }
  //      //        spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
  //      bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
  //      offset = offset+getCount
  //    }
  //    //删除当天及前50天的分区，重新插入(更新)
  //    for(date <- getBetweenDates(nowDate.plusDays(-50).toString,nowDate.plusDays(-1).toString)){
  //      //alter语句无法通过<和>号来删除多个分区，只能通过循环了。。。
  //    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition (date='"+date+"')")
  //    }
  //    println("重复分区删除完毕")
  //    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
  //  }

}
