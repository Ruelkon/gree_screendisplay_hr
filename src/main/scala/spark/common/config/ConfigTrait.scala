package spark.common.config

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import scala.xml.{Elem, XML}

/**
 * Author: 260371
 * Date: 2021/11/17
 * Time: 14:40
 * Created by: 聂嘉良
 */

/**
 * 通用配置类
 */
trait ConfigTrait {

  //判断当前操作系统
  val os: String = System.getProperty("os.name").toLowerCase()

  /*
   根据操作系统读取配置文件
   user.dir是yarn默认的用户目录，在spark提交时要把配置文件放在工作区里面，这样文件就会被自动上传到该目录
   */

  //读取数据接入配置
  val data_config: Elem = XML.load(
    new InputStreamReader(
      if(os.indexOf("linux") >= 0)
        new FileInputStream(new File(System.getProperty("user.dir")+ "/data_config.xml"))
      else
        getClass.getClassLoader.getResourceAsStream("data_config.xml")
      , "UTF-8"))

  //读取通用配置
  val app_config = new Properties()
  app_config.load(
    new InputStreamReader(
      if(os.indexOf("linux") >= 0)
        new FileInputStream(new File(System.getProperty("user.dir") + "/app_config.properties"))
      else
        getClass.getClassLoader.getResourceAsStream("app_config.properties")
      , "UTF-8"))

  /**
   * 解析字符串，若字符串中包含"${abc}"格式的值则按环境变量解析成字符串
   */
  def parseValue(value: String): String ={

    val result = new StringBuffer()

    val matcher = "\\$\\{(\\S+?)}".r.pattern.matcher(value)

    while (matcher.find) {

      val replacement = System.getProperty(matcher.group(1))
        .replaceAll("""\\""", "/")  //D:\IdeaProjects\spark-base 替换为 D:/IdeaProjects/spark-base

      matcher.appendReplacement(result, replacement)
    }
    matcher.appendTail(result)
    result.toString
  }
}



