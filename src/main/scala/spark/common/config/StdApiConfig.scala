package spark.common.config

import java.math.BigInteger
import java.security.MessageDigest

import scala.xml.NodeSeq

/**
 * Author: 260371
 * Date: 2021/11/22
 * Time: 19:20
 * Created by: 聂嘉良
 */
class StdApiConfig(val dataUrl: String) extends ApiConfigTrait{

  override val url: String = dataUrl

  override val method: String = confs \ "method" text

  override val contentType: String = confs \ "Content-Type" text

  override val params: Array[String] = (confs \ "params" text).split(" ")

  /**
   * 找到config.xml中对应的配置节点
   */
  def confs: NodeSeq = {
    (allApi.filter(node => (node \ "@id").text equals "std_api") \ "api")
      .filter(node => (dataUrl contains (node \ "@id").text) || ((node \ "@id").text equals "_")) match {
      case nodeSeq if nodeSeq.size>1 => nodeSeq head
      case nodeSeq => nodeSeq
    }
  }

  def checksum(a: String, b: String, c: String): String =
    new BigInteger(1, MessageDigest.getInstance("MD5").digest(s"${a}_${b}_$c".getBytes("UTF-8"))).toString(16)

  def tokenHeader = Map(
    ("x-now", System.currentTimeMillis.toString),
    ("x-username", authConf("username")),
    ("x-checksum", checksum(authConf("username"), authConf("password"), authConf("secureKey"))),
    ("Content-Type", tokenConf("Content-Type")))

  def dataHeader(token: String, callerInstance: String) = Map(
    ("x-now", System.currentTimeMillis().toString),
    ("x-username", callerInstance),
    ("x-checksum", checksum(callerInstance, System.currentTimeMillis.toString, authConf("secureKey"))),
    ("x-token", token),
    ("Content-Type", contentType))

}