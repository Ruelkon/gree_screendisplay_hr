package spark.common.config

import scala.xml.NodeSeq

/**
 * Author: 260371
 * Date: 2021/11/22
 * Time: 19:19
 * Created by: 聂嘉良
 */


/**
 * http接口的通用配置类
 */
trait ApiConfigTrait extends ConfigTrait{

  val allApi: NodeSeq = data_config \ "apis" \ "type"



  val url: String

  val method: String

  val contentType: String

  val params: Array[String]

  var authConf: Map[String, String] = (confs \ "auth" \ "_").map(node => (node.label, node.text)).toMap

  var tokenConf: Map[String, String] = (confs \ "token" \ "_").map(node => (node.label, node.text)).toMap

  /**
   * 找到config.xml中对应的配置节点
   */
  def confs: NodeSeq
}
