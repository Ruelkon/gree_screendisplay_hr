package spark.dao.api.impl

import java.net.URI

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.SparkSession
import spark.common.config.StdApiConfig
import spark.dao.api.BaseApiDao
import spark.util.{DateUtil, HttpRequest}

/**
 * Author: 260371
 * Date: 2021/11/17
 * Time: 14:22
 * Created by: 聂嘉良
 */
class StdApiDaoImpl(_spark: SparkSession,
                    val dataUrl: String,
                    db: String,
                    tableName: String,
                    tableType: String = "hive",
                    gradient: Int = 10,
                    override val pageSize: Int = 1000,
                    startDate: String = "2019-01-01",
                    endDate: String = DateUtil.getTodayDate) extends BaseApiDao(db, tableName, gradient, tableType, pageSize) {

  import scala.collection.JavaConversions._

  override val spark: SparkSession = _spark

  override val conf = new StdApiConfig(dataUrl)

  var token: JSONObject = JSON.parseObject(httpUtil.post(client, conf.tokenConf("url"), conf.tokenHeader, new JSONObject().fluentPut("password", conf.authConf("password")).toString)).getJSONObject("token")

  //  logger.info(token)
  def refreshToken(): Unit = {
    token = JSON.parseObject(httpUtil.post(client, conf.tokenConf("url"), conf.tokenHeader, new JSONObject().fluentPut("password", conf.authConf("password")).toString)).getJSONObject("token")
  }

  def getRequestEachPage(page: Int, socketTimeout: Int = 1000 * 60 * 20, connectTimeout: Int = 1000 * 60 * 5, connectionRequestTimeout: Int = 1000 * 60 * 5): HttpRequest = {
    val params = new JSONObject()
    conf.params.foreach {
      case "offset" => params.fluentPut("offset", ((page - 1) * pageSize + 1).toString)
      case "count" => params.fluentPut("count", pageSize.toString)
      case "startdate" => params.fluentPut("startdate", startDate.replaceAll("-", "").toInt)
      case "enddate" => params.fluentPut("enddate", endDate.replaceAll("-", "").toInt)
      case other => params.fluentPut(other, "")
    }
    val dataHeader = conf.dataHeader(token.getString("token"), token.getString("callerInstance"))

    conf.method match {
      case "post" =>
        val httpPost = new HttpPost()
        val config = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout)
          .setConnectTimeout(connectTimeout).build()
        httpPost.setConfig(config)
        httpPost.setURI(new URI(conf.dataUrl))
        dataHeader.foreach {
          case (key, value) => httpPost.setHeader(key, value)
        }
        httpPost.setEntity(new StringEntity(params.toString))
        new HttpRequest(httpPost)
      case "get" =>
        val httpGet = new HttpGet()
        val config = RequestConfig.custom()
          .setSocketTimeout(socketTimeout)
          .setConnectionRequestTimeout(connectionRequestTimeout)
          .setConnectTimeout(connectTimeout)
          .build()
        httpGet.setConfig(config)
        val uriSb = new StringBuilder(dataUrl + "?")
        params.foreach {
          case (k, v) => uriSb.append(k).append("=").append(v.toString).append("&")
        }
        httpGet.setURI(new URI(uriSb.toString()))
        dataHeader.foreach {
          case (key, value) => httpGet.setHeader(key, value)
        }
        new HttpRequest(httpGet)
    }
  }

  def getDataInJson(jsonResult: String): String = {
    val data = JSON.parseObject(jsonResult).getString("data")

    if(data == null) throw new NullPointerException()

    data
  }
}
