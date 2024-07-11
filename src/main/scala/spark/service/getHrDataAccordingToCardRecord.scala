package spark.service

/**
 * 这个类是最终实现类，更新当天的cardRecord和dayAttendance表（每半小时执行一次）
 * 另外也可以单独实现card_record的32天数据的更新，一天更新两次
 */

import java.io.{File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import spark.common.Constants.{os, parseValue}
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{ActorUtil, HttpUtils, TableImportUtil}

import scala.Console.println
import scala.collection.mutable.ListBuffer

class getHrDataAccordingToCardRecord(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass)
  def logic(): Unit = {
    //程序发布时需要注释
//    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
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
        //cardrecord表
        val https2 = "https://hrapi.gree.com/apiv2/attendance/cardrecordlist"
        //按时间段更新数据,需要设置起始日期参数
//        jsonSinkHive(https2,"SignDate","hr_card_record","SignDate")
        //更新当天数据，用来在半小时一更中使用
        new TableImportUtil(spark).updateRecordTable(
        jsonSinkHiveCurrentDay(https2,"hr_card_record","signdate")
          ,"hr_card_record"
//        jsonSinkHiveFiftyday(https2,"SignDate","hr_card_record","signdate")
        )
      }, "ex2", errList)
    actorUtil.doSthAndCollectException(
      {
        //day_attendance表
        val https3 = "https://hrapi.gree.com/apiv2/attendance/interface21"
        //按时间段更新数据,需要设置起始日期参数，生效参数是startDate和endData，指定日期参数无效
        //        jsonSinkHive(https3,"Date","hr_day_attendance","Date")
        //按天更新，需要设置指定日期参数,生效参数是getHrDateAccordingToDate.oneDay，时间段参数无效
        //        jsonSinkHiveOneDay(https3,"Date","hr_day_attendance","Date")
        //该方法按天更新，用来在半小时一更中使用,目前没有这个更新的需求
//        jsonSinkHiveCurrentDay(https3,"Date","hr_day_attendance","date")
      }, "ex3", errList)

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
   * 按天更新今天的分区
   * @param https
   * @param partitionName json中时间字段的名称，每个表不一样
   * @param tableName
   */
  def jsonSinkHiveCurrentDay(https:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
    val sparkSession = new HiveDaoImpl(spark)
    var getCount = 5000
    //获取当天数据
     val date = nowDate.toString
    println(date)
    //按天分批存到hive分区
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
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
      bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
      offset = offset+getCount
    }
    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
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
//        spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
        bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
        offset = offset+getCount
      }
    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
  }


  /**
   * 按照起始日期将数据分批插入到分区中
   * @param https
   * @param dateName json中时间字段的名称，每个表不一样
   * @param tableName
   * @param severalDays 表示更新几天的数据，如50天，32天
   */
  def jsonSinkHiveSeveralday(https:String,dateName:String,tableName:String,partitionName:String,severalDays:Int): Unit ={
    val nowDate = LocalDate.now()
    //起始日期：当前系统时间的前1天
    val startDate: String = nowDate.plusDays(-severalDays).toString
    //截止日期：当前系统时间的前50天
    val endDate: String = nowDate.plusDays(-1).toString

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
        //        spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
        bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
        offset = offset+getCount
      }
      spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }


}
