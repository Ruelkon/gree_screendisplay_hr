package spark.dao.api.impl

import java.net.URI

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.spark.sql.SparkSession
import spark.common.config.MesApiConfig
import spark.dao.api.BaseApiDao
import spark.util.{DateUtil, HttpRequest}

import scala.collection.mutable.ListBuffer

/**
 * Author: 260371
 * Date: 2021/11/18
 * Time: 15:58
 * Created by: 聂嘉良
 */
class MesApiDaoImpl(_spark: SparkSession,
                    val dataUrl: String,
                    db: String,
                    tableName: String,
                    tableType: String = "hive",
                    gradient: Int = 1,
                    val beginDate: String = "2019-01-01",
                    val endDate: String = DateUtil.getTodayDate) extends BaseApiDao(db, tableName, gradient, tableType) {

  import scala.collection.JavaConversions._

  override val spark: SparkSession = _spark

  override val conf: MesApiConfig = new MesApiConfig(dataUrl)

  var token: JSONObject = JSON.parseObject(httpUtil.post(conf.tokenConf("url"), conf.tokenHeader, conf.authConf("authStr"), "gb2312"))

  val header: Map[String, String] = conf.dataHeader(token.getString("token_type"), token.getString("access_token"))

  //  logger.info(header)
  //  logger.info(param)

  def getRequestEachPage(page: Int, socketTimeout: Int = 1000 * 60 * 20, connectTimeout: Int = 1000 * 5, connectionRequestTimeout: Int = 1000 * 5): HttpRequest = {
    val fromDate = DateUtil.dateAdd(beginDate, (page - 1) * 60) + " 00:00:00"
    val toDate = DateUtil.dateAdd(fromDate, 59) + " 00:00:00"
    val httpPosts = new ListBuffer[HttpPost]

    if (fromDate < endDate) {
      val dataHeader = conf.dataHeader(token.getString("token_type"), token.getString("access_token"))
      val erpids = Seq(400, 500, 339, 358, 300, 560, 677, 610, 751)
      for (erpid <- erpids) {
        val param = new JSONObject().fluentPut("fromDate", fromDate).fluentPut("toDate", toDate).fluentPut("CompanyID", erpid)
        logger.info(param)
        val httpPost = new HttpPost()
        val config = RequestConfig.custom().setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout)
          .setConnectTimeout(connectTimeout).build()
        httpPost.setConfig(config)
        httpPost.setURI(new URI(conf.dataUrl))
        dataHeader.foreach {
          case (key, value) => httpPost.setHeader(key, value)
        }
        httpPost.setEntity(new StringEntity(param.toString, ContentType.create("application/json", "UTF-8")))
        httpPosts.append(httpPost)
      }
    } else {
      finish = true
    }
    new HttpRequest(httpPosts.toArray)
  }

  def getDataInJson(jsonResult: String): String = {
    val result = JSON.parseObject(jsonResult).getString("result")

    if(result == null) throw new NullPointerException()

    result
  }

}
