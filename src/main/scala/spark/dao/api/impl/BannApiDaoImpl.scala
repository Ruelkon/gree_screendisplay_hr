package spark.dao.api.impl

import java.net.URI

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.SparkSession
import spark.common.config.BannApiConfig
import spark.dao.api.BaseApiDao
import spark.util.{DateUtil, HttpRequest}

/**
 * Author: 260371
 * Date: 2021/11/18
 * Time: 15:24
 * Created by: 聂嘉良
 */

/**
 *
 * @param _spark
 * @param dataUrl
 * @param db 要存入的库名
 * @param tableName 表名
 * @param tableType hive/kudu
 * @param gradient 每个线程一次取多少页
 * @param pageSize 每页的数据量
 */
class BannApiDaoImpl(_spark: SparkSession,
                     val dataUrl: String,
                     db: String, tableName: String,
                     tableType: String = "hive",
                     gradient: Int=10,
                     override val pageSize: Int=1000) extends BaseApiDao(db, tableName, gradient, tableType) {

  import scala.collection.JavaConversions._

  override val spark: SparkSession = _spark

  override val conf = new BannApiConfig(dataUrl)

  var (token, tokenTime) = (JSON.parseObject(httpUtil.get(conf.tokenConf("url"), conf.tokenHeader, JSON.toJSON({
    val toJavaMap = (javaMap: java.util.Map[String, String]) => javaMap
    toJavaMap(conf.authConf)
  }).asInstanceOf[JSONObject])).getString("access_token"), System.currentTimeMillis())

  def refreshToken(): Unit = {
    synchronized{
      token = JSON.parseObject(httpUtil.get(conf.tokenConf("url"), conf.tokenHeader, JSON.toJSON({
        val toJavaMap = (javaMap: java.util.Map[String, String]) => javaMap
        toJavaMap(conf.authConf)
      }).asInstanceOf[JSONObject])).getString("access_token")
      tokenTime = System.currentTimeMillis()
    }
  }

  def getRequestEachPage(page: Int, socketTimeout: Int = 1000*60*20, connectTimeout: Int = 1000*60*3, connectionRequestTimeout: Int = 1000*60): HttpRequest = {
    val params = new JSONObject()
    conf.params.foreach(param => params.put(param, ""))

    //1.5小时刷新一次token
    val timeNow = System.currentTimeMillis()
    if((timeNow - tokenTime) / 1000 / 60 / 60 >= 1.5){
      refreshToken()
      logger.info(s"${DateUtil.unixToTime(String.valueOf(tokenTime).substring(0, 10))}  更新token，token更新为: $token")
    }
    val dataHeader = conf.dataHeader(token)

    conf.method match {
      case "post" =>
        val httpPost = new HttpPost()
        val config = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout)
          .setConnectTimeout(connectTimeout).build()
        httpPost.setConfig(config)
        httpPost.setURI(new URI(conf.url+"?pageNum="+page+"&pageSize="+pageSize))
        dataHeader.foreach{
          case (key, value) => httpPost.setHeader(key, value)
        }
        httpPost.setEntity(new StringEntity(params.toString))
        new HttpRequest(httpPost)
      case "get" =>
        val httpGet = new HttpGet()
        val config = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout)
          .setConnectTimeout(connectTimeout).build()
        httpGet.setConfig(config)
        val uriSb = new StringBuilder(conf.url+"?pageNum="+page+"&pageSize="+pageSize+"?")
        params.foreach{
          case (k, v) => uriSb.append(k).append("=").append(v.toString).append("&")
        }
        httpGet.setURI(new URI(uriSb.toString()))
        dataHeader.foreach{
          case (key, value) => httpGet.setHeader(key, value)
        }
        new HttpRequest(httpGet)
    }
  }

  def getDataInJson(jsonResult: String): String = {
    val data = JSON.parseObject(jsonResult).getJSONObject("data")

    if(data == null) throw new NullPointerException()

    val records = data.getString("records")

    if(records == null) throw new NullPointerException()

    records
  }

}
