package spark.service

//该用来刚从接口接入表时获取全量数据。具体后期按天或者时间段获取时间段就用getHrDataAccordingToDate类。
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import spark.common.Constants
import spark.dao.hive.impl.HiveDaoImpl
import spark.util.{HttpUtils, KerberosUtil}

import scala.Console.println

class getHrDataAccordingToDept(spark: SparkSession) {

  def logic(): Unit = {
//    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)

    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    val start = System.currentTimeMillis()
    parallelInsert()
    val end = System.currentTimeMillis()
    println("花费了"+(end - start)+"ms")
    println("查询成功")
  }

  //并行写入
  def parallelInsert(): Unit ={
    //dept_info
    val https = "https://hrapi.gree.com/apiv2/attendance/deptinfolist"
    jsonSinkHiveAllDate(https,"hr_dept_info")
  }
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
      bigDataframe = bigDataframe.unionAll(spark.read.json(spark.sparkContext.parallelize(Seq(JSON.toJSONString(jsonObject.getJSONArray("items"),SerializerFeature.WriteMapNullValue)))))
      offset = offset+5000
    }
    //todo 加一个每批的计数
    sparkSession.write("gree_screendisplay_hr", tableName, bigDataframe, SaveMode.Append)
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
}
