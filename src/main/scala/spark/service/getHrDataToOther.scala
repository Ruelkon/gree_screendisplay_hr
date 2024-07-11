package spark.service

//对应jar包中的spark-other2hive.jar
import java.io.{File, FileInputStream, InputStreamReader}
import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.Console.println
import scala.collection.mutable.ListBuffer
import cn.hutool.http.HttpUtil
import java.text.SimpleDateFormat

import spark.common.Constants
import spark.common.Constants.{os, parseValue}
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{ActorUtil, HttpUtils, KerberosUtil, TableImportUtil}


/***
 * 后期新接入的接口，包括全量数据和更新数据。本质和update50AndBase的代码一致，只是当时在一个类里同时接入多张表容易出问题，所以单独开了一个类
 */
class getHrDataToOther(spark: SparkSession)  {
  val logger: Logger = Logger.getLogger(getClass)
  def logic(): Unit = {
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
  def parallelInsert(): Unit = {
    val actorUtil = new ActorUtil
    val errList = new ListBuffer[String]
    actorUtil.doSthAndCollectException(
      {
        //排班表：表
        val https = "https://hrapi.gree.com/apiv2/attendance/interface39"
        //排班表需要先overwrite再append
        new TableImportUtil(spark).updateRecordTable(
//          jsonSinkHiveToPaiBan(https,"YM","hr_emp_paiban","ym"),"hr_emp_paiban"
          jsonSinkHiveToPaiBan(https,"hr_emp_paiban","ym"),"hr_emp_paiban"
        )
      }, "ex1", errList)
    actorUtil.doSthAndCollectException(
      {
        //请假出差表：表
        val https = "https://hrapi.gree.com/apiv2/attendance/attleavelist"
          //全量更新
//              jsonSinkHiveUseBetween(https,"endDate", "hr_att_leave","enddate")
          //局部更新45天
        new TableImportUtil(spark).updateRecordTable(
//          jsonSinkHive45(https,"endDate", "hr_att_leave","enddate"),"hr_att_leave"
          jsonSinkHive45(https,"hr_att_leave","enddate"),"hr_att_leave"
        )
      }, "ex1", errList)
    actorUtil.doSthAndCollectException(
      {
        //请假出差类型表：表
        val https = "https://hrapi.gree.com/apiv2/attendance/attleavetypelist"
        new TableImportUtil(spark).updateRecordTable(
        jsonSinkHiveAllDate(https, "hr_att_leave_type",""), "hr_att_leave_type"
        )
      }, "ex1", errList)
    actorUtil.doSthAndCollectException(
      {
//        未审批请假：hr_leaver_unapprovied表,get请求
//        val https = "http://mobileapi.gree.com/wfapi/api/DataForm/4097?take=5000&&Filter=费用系统状态='审批中' or (IsClosed='False' and 时长>0)"
        val tableName = "hr_leave_unapprovied"
        //清空表后重新插入,spark中可能不能用if exists
        spark.sql("truncate table gree_screendisplay_hr."+tableName)
        //or条件拆分
        val systemStatusFilter = "&Filter=%E8%B4%B9%E7%94%A8%E7%B3%BB%E7%BB%9F%E7%8A%B6%E6%80%81=%27%E5%AE%A1%E6%89%B9%E4%B8%AD%27%20"
//        val systemStatusFilter = "&Filter=费用系统状态='审批中'"
        val isClosedFilter = "&Filter=%20(IsClosed=%27False%27%20and%20%E6%97%B6%E9%95%BF%3E0)"
//        val isClosedFilter = "&Filter=IsClosed='False' and 时长>0"
        //分成两次调用，因为接口无法使用or，只能分开调用从接口取数据
        new TableImportUtil(spark).updateRecordTable(
          jsonSinkHiveAllDate_get(tableName,"",systemStatusFilter)
    ,tableName
        )
        new TableImportUtil(spark).updateRecordTable(
          jsonSinkHiveAllDate_get(tableName,"",isClosedFilter)
    ,tableName
        )
      }, "ex1", errList)
    actorUtil.doSthAndCollectException(
      {
//        //加班记录表：表
        val https = "https://hrapi.gree.com/apiv2/ot/interface14"
          //全量更新
//          jsonSinkHiveAllDate(https, "hr_work_overtime","workdate")
          //局部更新45天
        new TableImportUtil(spark).updateRecordTable(
//          jsonSinkHive45(https,"workDate", "hr_work_overtime","workdate"), "hr_work_overtime"
          jsonSinkHive45(https,"hr_work_overtime","workdate"), "hr_work_overtime"
        )
      }, "ex1", errList)
        actorUtil.doSthAndCollectException(
          {
            //考勤系统员工补卡记录表
            val https = "https://hrapi.gree.com/apiv2/attendance/interface40"
              //全量更新
//              jsonSinkHiveUseBetween(https,"hr_buka_record","signdate")
              //局部更新45天
            new TableImportUtil(spark).updateRecordTable(
              jsonSinkHive45(https,"hr_buka_record","signdate")
              , "hr_buka_record"
            )
          }, "ex1", errList)
    actorUtil.close
  }

  /**
   * 按ym年月字段存储数据
   * @param https
   * @param tableName
   * @param partitionName json中时间字段的名称，每个表不一样
   */
  def jsonSinkHiveToPaiBan(https:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyyMM")
//    //全量更新：起始月份202002时间之前没有数据了，所以从2月份开始
//    val startDate: String = "202002"
//    //截止日期：当前系统时间的年月份
//    val endDate: String = "202207"

    //局部更新：从当前月份到前两个月的数据进行更新
    val startDate: String = formatter.format(nowDate.plusMonths(-2))
    //    //截止日期：当前系统时间年月份(排班表特殊，如果下个月有数据结束日期要加上一个月，加判断)
    var endDate: String = formatter.format(nowDate)
    val getCount = 5000
    val count=  getJson(https,1,getCount,"\""+partitionName+"\":\"" + formatter.format(nowDate.plusMonths(1)) + "\"").getInteger("count")
    if(count>0)
      {
        //如果下个月有数据，endate加1
        endDate = formatter.format(nowDate.plusMonths(1))
      }

    val sparkSession = new HiveDaoImpl(spark)
    for(date <- getMonthBetween(startDate,endDate)) {
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
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0){
        bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
      //在做局部更新操作时该处要打开注释
       spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }

  /**
   * 按日期范围取数据（可全量也可部分取数）：通过between and拆分数据后存储,目前使用该策略接的表：hr_buka_record,hr_emp_paiban
   * @param https
   * @param tableName
   * @param partitionName json中时间字段的名称，每个表不一样
   */
  def jsonSinkHiveUseBetween(https:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    //    全量更新：起始日期当前系统时间的前若干天
    val start: String = "2020-10-26"
    //    截止日期：设置成当前系统时间的前1天
    val end: String = "2022-09-22"
//    val end = formatter.format(nowDate.plusDays(-1))

    val sparkSession = new HiveDaoImpl(spark)
    var getCount = 5000
    //这里的date格式是拼接成between and的方式，按照时间段来取数据
    for(date <- getBetween(start,end)) {
      println(date)
      //按天分批存到hive分区,下面的日期是初始值，只要保证当天有数据就行
//      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https,date = "\"endDate\":\"BETWEEN 2022-01-01 AND 2022-01-01\"").getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https,date = "\""+partitionName+"\":\"BETWEEN 2022-01-01 AND 2022-01-01\"").getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
            intDataframe.show()
      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
      var offset = 1
      var insert = true
      while(insert){
        var jsonObject =  getJson(https,offset,getCount,"\""+partitionName+"\":\"" + date + "\"")
        //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
        if (jsonObject.getInteger("count")<getCount){
          insert = false
        }
        //根据条件获取的dataframe
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        //如果获取的dataframe不为空才能和bigDataframe做union操作，不然如果没数据的话会报列数不一致的异常
        if(getDf.columns.size!=0){
        bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
      //在做局部更新操作时该处要打开注释
//      spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+date+"T00:00:00')")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }

  /**
   * 通过between and拆分数据后存储,更新-45天当天之后的所有数据(未来的数据也会自动接入，可以在使用jsonSinkHiveUseBetween更新今天之前的全量数据但是未来的数据没有更新的情况)，目前使用该更新策略的表：请假出差表(hr_att_leave)和加班表（hr_work_overtime）
   * @param https
   * @param tableName
   * @param partitionName json中时间字段的名称，每个表不一样
   */
  def jsonSinkHive45(https:String,tableName:String,partitionName:String): Unit ={
    val nowDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val sparkSession = new HiveDaoImpl(spark)
    var getCount = 5000
//    for(date <- getBetween(start,end)) {
    val date = formatter.format(nowDate.plusDays(-50))
//      println(date)
      //按天分批存到hive分区,下面的日期是初始值，只要保证当天有数据就行
      var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
           intDataframe.show()
    println("count==")
      var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
      var offset = 1
      var insert = true
      while(insert){
        //获取结束日期大于等于前45天那天的数据（即包括今天之后的数据如果有的话也会一起接入）
        var jsonObject =  getJson(https,offset,getCount,"\""+partitionName+"\":\">=" + date + "\"")
        //如果count的数量小于5000，表示接下来没有数据了，结束循环标记
        if (jsonObject.getInteger("count")<getCount){
          insert = false
        }
//      spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))).show()
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0){
          bigDataframe = bigDataframe.unionAll(getDf)
        }
        offset = offset+getCount
      }
    val partitionrdd = bigDataframe.select(partitionName).distinct().rdd.map(x => x(0))
    for(partition <- partitionrdd.collect()){
      //因为批量删除分区在spark3.0.0之前无法><=同时删除多个分区，只能进行遍历，并且在客户端运行。使用在做局部更新操作时该处要打开注释
    spark.sql("alter table gree_screendisplay_hr."+tableName+" drop if exists partition ("+partitionName+"='"+partition+"')")
      println(partition+"+++")
    }
    println(bigDataframe.count()+"count==")
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
//    }
  }

  /**
   * 全量按批存hive，不分区全量取数据,适用于接口数据量不大的数据接入（post请求）。
   * @param https
   * @param tableName
   * @param partitionName 如果没有分区赋值为""即可
   */
  def jsonSinkHiveAllDate(https:String,tableName:String,partitionName:String): Unit ={
    val sparkSession = new HiveDaoImpl(spark)
    //清空表后重新插入，spark中可能不能用if exists
    spark.sql("truncate table gree_screendisplay_hr."+tableName+"")
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(getJson(https).getJSONArray("items"), SerializerFeature.WriteMapNullValue))))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var offset = 1
    var insert = true
    while(insert){
//    for(i <- 1 to 2){
      var jsonObject =  getJson(https,offset,5000)
      //如果剩下的数量少于5000，结束循环标记
      if (jsonObject.getInteger("count")<5000){
        insert = false
      }
      val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
      if(getDf.columns.size!=0)
        {
      bigDataframe = bigDataframe.unionAll(getDf)
        }
      offset = offset+5000
    }
    bigDataframe.show(10)
    println("总数量："+bigDataframe.count())
    //todo 加一个每批的计数
    if(partitionName == ""){
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append)
    }
    else {
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
//      bigDataframe.write.mode(SaveMode.Append).partitionBy(partitionName).saveAsTable(tableName)
    }
  }

  /**
   * 全量按批存hive，按分区分批全接入,适用于接口数据量大的数据接入。
   * @param https
   * @param dateName  接口中日期字段
   * @param tableName
   */
  def jsonSinkHiveAllDateBig(https:String,dateName:String,tableName:String,partitionName:String): Unit ={
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
      //按天分批存到hive分区，这里的SignDate要根据分区字段修改！
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
        val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue))))
        if(getDf.columns.size!=0)
        {
          bigDataframe = bigDataframe.unionAll(getDf)
        }
         offset = offset+getCount
      }
      sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append,Array(partitionName))
    }
  }

  /**
   * 全量按批存hive，单独全量存入hive，不然按照索引存会出现重复数据（get请求）。
   * @param tableName
   * @param partitionName 如果没有分区赋值为""即可
   */
  def jsonSinkHiveAllDate_get(tableName:String,partitionName:String,filterString:String): Unit ={
    val sparkSession = new HiveDaoImpl(spark)
//    //清空表后重新插入,spark中可能不能用if exists
//    spark.sql("truncate table gree_screendisplay_hr."+tableName+"")
    var intDataframe = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonFromGet(1,null,filterString).getJSONArray("Result"), SerializerFeature.WriteMapNullValue))))
    var bigDataframe = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], intDataframe.schema)
    var skipNum = 0  //表示要跳过多少条数据不读取，类似于偏移量
    var takeTotalNum = 5000 //表示总的拿取数据量，
    var insert = true
    while(insert){
      var jsonObject =  jsonFromGet(takeTotalNum,skipNum,filterString)  //eg: 像5000,0和10000,5000 按批存
      var count = jsonObject.getInteger("Count")
      //如果剩下的数量少于5000，结束循环标记
      if (count<5000){
        insert = false
      }
      val getDf = spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("Result"),SerializerFeature.WriteMapNullValue))))
      bigDataframe.show()
      getDf.show(10)
      println(getDf.columns.size+"===")
      if(getDf.columns.size!=0){
      bigDataframe = bigDataframe.unionAll(getDf)
      }
      skipNum = skipNum+takeTotalNum  //每次跳过多少条数据
      takeTotalNum = takeTotalNum+5000  //每拿一次拿5000条数据
    }
    var newDf = bigDataframe
      .withColumnRenamed("HR公司","companycode")
      .withColumnRenamed("HR部门","deptcode")
      .withColumnRenamed("HR科室","officecode")
      .withColumnRenamed("HR工号","empnum")
      .withColumnRenamed("提交人邮箱号","mailnum")
      .withColumnRenamed("开始时间","start")
      .withColumnRenamed("结束时间","end")
      .withColumnRenamed("请假类型代码","leavecode")
      newDf.show(10)
      sparkSession.write("gree_screendisplay_hr", tableName, newDf, SaveMode.Append)
  }

  /**
   * @description 根据起始日期拼接between and
   * @author xiong
   * @date 2020/8/24 16:48
   * @param minDate
   * @param maxDate
   * @return java.util.List<java.lang.String>
   */
  def getBetween(minDate: String, maxDate: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    var buffer = new ListBuffer[String]
    val c = Calendar.getInstance
    val d = Calendar.getInstance
    c.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(minDate))
    d.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(maxDate))
    //起始日期~起始日期之后一个月 为一个阶段，起始日期后一个月+1~后一个月，直到结束日期
    var start = ""
    var end = ""
    while (c.before(d)){
      start = format.format(c.getTime())
      c.add(Calendar.MONTH, 1)
      end = format.format(c.getTime())
      if(end<maxDate){
        buffer+="BETWEEN "+start+" AND "+end
        //在end日期的基础上加一天
        c.add(Calendar.DATE, 1)
      }
    }
    //从起始日期到剩下的终止日期
    buffer+="BETWEEN "+start+" AND "+maxDate
    buffer.toList
  }

  /**
   * @description 获取两个日期之间的月份，仅供排班表使用
   * @author tank
   * @date 2019/8/24 16:48
   * @param minDate
   * @param maxDate
   * @return java.util.List<java.lang.String>
   */
  def getMonthBetween(minDate: String, maxDate: String) = {

    var buffer = new ListBuffer[String]
    val format = new SimpleDateFormat("yyyyMM")
    val c = Calendar.getInstance
    val d = Calendar.getInstance
    c.setTime(new SimpleDateFormat("yyyyMM").parse(minDate))
    d.setTime(new SimpleDateFormat("yyyyMM").parse(maxDate))
    while (c.before(d)){
      buffer += format.format(c.getTime())
      c.add(Calendar.MONTH, 1)
    }
    //包含末尾的月份
    buffer+=format.format(d.getTime())
    buffer.toList
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

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
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
  /**
   *
   * @param num  从接口获得的数量，最多5000条
   * @param skip 跳过前几条数据
   * @return
   */
  def jsonFromGet(num: Int, skip: Integer,filterString:String): JSONObject={
    val headerMap = new util.HashMap[String, String]
    headerMap.put("appID", "4a2b4978-83bf-4172-9a4a-28d063624fd8")
    headerMap.put("appKey", "cd4f3413-9868-4e77-b31e-00fa5ee621cd")
    //http://mobileapi.gree.com/wfapi/api/DataForm/4097?take=5000&Filter=费用系统状态='审批中' or (IsClosed='False' and 时长>0)
//    "&Filter=%E8%B4%B9%E7%94%A8%E7%B3%BB%E7%BB%9F%E7%8A%B6%E6%80%81=%27%E5%AE%A1%E6%89%B9%E4%B8%AD%27%20or&Filter=%20(IsClosed=%27False%27%20and%20%E6%97%B6%E9%95%BF%3E0)"
    var url = "http://mobileapi.gree.com/wfapi/api/DataForm/4097?take=" + num + filterString
    if (skip != null) {
      url = "http://mobileapi.gree.com/wfapi/api/DataForm/4097?take=" + num + "&skip=" + skip + filterString
    }
        //    println ("获取的json数据为："+httpUtil.post(https, map, bodyString))
    val itemJsonArray = JSON.parseObject(httpGet(url, headerMap))
    itemJsonArray
  }

  //发送httpget请求,参数需要在发送到这个请求之前就已经配置好,这样才能发送请求//发送httpget请求,参数需要在发送到这个请求之前就已经配置好,这样才能发送请求
  def httpGet(https: String, headerMap: util.HashMap[String, String]): String = {
    var content = null
    try {
      val request = HttpUtil.createGet(https)
      import scala.collection.JavaConversions._
      for (key <- headerMap.keySet) {
        request.header(key, headerMap.get(key))
      }
      request.execute.body()
    }catch {
      case e:Exception=>{
        "http get请求失败：" + e
      }
    }
  }


}
