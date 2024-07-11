package spark.common.config

import scala.xml.NodeSeq

/**
 * Author: 260371
 * Date: 2021/11/22
 * Time: 19:21
 * Created by: 聂嘉良
 */
class BannApiConfig(val dataUrl: String) extends ApiConfigTrait {

  override val url: String = dataUrl

  override val method: String = confs \ "method" text

  override val contentType: String = confs \ "Content-Type" text

  override val params: Array[String] = (confs \ "params" text).split(" ")

  def tokenHeader = Map(("Content-Type", tokenConf("Content-Type")))

  def dataHeader(token: String) = Map(("Content-Type",contentType), ("x-authorization", token))

  /**
   * 找到config.xml中对应的配置节点
   */
  override def confs: NodeSeq = {
    (allApi.filter(node => (node \ "@id").text equals "bann_api") \ "api")
      .filter(node => (dataUrl contains (node \ "@id").text) || ((node \ "@id").text equals "_")) match {
      case nodeSeq if nodeSeq.size>1 => nodeSeq head
      case nodeSeq => nodeSeq
    }
  }
}
