package spark.service

/**
 * 1、这个类是初始化empinfo用的，按时间段更新emp表
 * 2、也可以指定某天更新
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
import spark.util.HttpUtils

import scala.Console.println
import scala.collection.mutable.ListBuffer

class getHrDataAccordingToEmp(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass)

  def logic(): Unit = {
//        new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    val start = System.currentTimeMillis()
    parallelInsert()
    val end = System.currentTimeMillis()
    println("花费了" + (end - start) + "ms")
    println("查询成功")
  }

  //并行写入
  def parallelInsert(): Unit = {
//    val actorUtil = new ActorUtil
//    val errList = new ListBuffer[String]
//    actorUtil.doSthAndCollectException(
//      {
        //emp_info
        val https1 = "https://hrapi.gree.com/apiv2/attendance/empinfolist"
    //全量更新emp
//    jsonSinkHiveAllToEmpAll(https1, "hr_emp_info", "date")
    //按当天或者指定一天更新emp,
    jsonSinkHiveAllToEmpOneday(https1, "hr_emp_info", "date")
//      }, "ex1", errList)
//
//    println("输入成功")
//    logger.error(errList)
//    actorUtil.close
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
  def getJson(https: String, beginIndex: Int = 1, getCount: Int = 1, date: String = ""): JSONObject = {
    val httpUtil = new HttpUtils()
    val client: CloseableHttpClient = httpUtil.createSSLClientDefault()
    //从接口获取数据（全量？如何分批）
    var bodyString = ""
    if (date == "") {
      bodyString = "{\"AppID\":\"4E74E0EA3E76479B89207B2F1E9A4329\",\"Secret\":\"1A6A4C58784C4942B4CD5106DCA74E26\",\"Begin\":" + beginIndex + ",\"Count\":" + getCount + "}"
    } else {
      bodyString = "{\"AppID\":\"4E74E0EA3E76479B89207B2F1E9A4329\",\"Secret\":\"1A6A4C58784C4942B4CD5106DCA74E26\"," + date + ", \"Begin\":" + beginIndex + ",\"Count\":" + getCount + "}"
    }
    val map = new util.HashMap[String, String]
    map.put("Content-Type", "application/json")
    //    println ("获取的json数据为："+httpUtil.post(https, map, bodyString))
    val itemJsonArray = JSON.parseObject(httpUtil.post(client, https, map, bodyString))
    itemJsonArray
  }


  /**
   * 获取两个日期之间的日期
   *
   * @param start 开始日期
   * @param end   结束日期
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
   * 按天全量按批存hive，分区,针对empinfo表,
   *
   * @param https
   * @param tableName
   */
  def jsonSinkHiveAllToEmpOneday(https: String, tableName: String, partitionName: String): Unit = {
    val nowDate = LocalDate.now()
//    val date = nowDate.plusDays(-1).toString + "T00:00:00"
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
    //如果日期为空，则从json获取当前日期前一天插入hive,因为当天日期的数据不完全。
    if(date == "null"){
      date = nowDate.plusDays(-2).toString
    }
    val sparkSession = new HiveDaoImpl(spark)
    //按offset存到hive
    //从spark中读取表的条数，条数加1就是偏移(这种方式是每次调用方法自动更新数据):前提是接口中的数据顺序是不变的
    var insertCount = spark.sql("select * from gree_screendisplay_hr." + tableName + " where `date`='" + date + "'").count().toInt
    println(tableName + "表当前数据量：" + insertCount)
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date", lit(date))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var offset = insertCount + 1
    var insert = true
    while (insert) {
      var jsonObject = getJson(https, offset, 5000)
      //如果剩下的数量少于5000，结束循环标记
      if (jsonObject.getInteger("count") < 5000) {
        insert = false
      }
      bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date", lit(date)))
      offset = offset + 5000
    }
    //empinfo初始化表，需要对照日考勤表（day_attendance)，初始化对应日期的emp信息
    bigDataframe.cache()
    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
    bigDataframe=bigDataframe.withColumn("date", lit(date+"T00:00:00"))
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append, Array(partitionName))
  }

  /**
   * 按天全量按批存hive，分区,针对empinfo表,
   *
   * @param https
   * @param tableName
   */
  def jsonSinkHiveAllToEmpAll(https: String, tableName: String, partitionName: String): Unit = {
    val nowDate = LocalDate.now()
    val date = nowDate.plusDays(-1).toString + "T00:00:00"
    val sparkSession = new HiveDaoImpl(spark)
    //按offset存到hive
    //从spark中读取表的条数，条数加1就是偏移(这种方式是每次调用方法自动更新数据):前提是接口中的数据顺序是不变的
    var insertCount = spark.sql("select * from gree_screendisplay_hr." + tableName + " where `date`='" + date + "'").count().toInt
    println(tableName + "表当前数据量：" + insertCount)
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date", lit(date))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var offset = insertCount + 1
    var insert = true
    while (insert) {
      var jsonObject = getJson(https, offset, 5000)
      //如果剩下的数量少于5000，结束循环标记
      if (jsonObject.getInteger("count") < 5000) {
        insert = false
      }
      bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"), SerializerFeature.WriteMapNullValue)))).withColumn("date", lit(date)))
      offset = offset + 5000
    }
    //empinfo初始化表，需要对照日考勤表（day_attendance)，初始化对应日期的emp信息
    //读取通用配置
    val app_config = new Properties()
    app_config.load(
      new InputStreamReader(
        if(os.indexOf("linux") >= 0)
          new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
        else
          getClass.getClassLoader.getResourceAsStream("app_config.properties")
        , "UTF-8"))
    val startDate = parseValue(app_config.getProperty("getHrDateAccordingToDate.startDate"))
    val endDate: String = parseValue(app_config.getProperty("getHrDateAccordingToDate.endDate"))
    bigDataframe.cache()
    for (date <- getBetweenDates(startDate, endDate)) {
      {
        bigDataframe=bigDataframe.withColumn("date", lit(date+"T00:00:00"))
      //todo 加一个每批的计数
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append, Array(partitionName))
      }
    }
  }

}
