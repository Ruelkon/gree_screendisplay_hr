package spark.common

import java.io.File

import spark.common.config.ConfigTrait

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 9:34
 * Created by: 聂嘉良
 */

object Constants extends ConfigTrait {

  //测试Hive
  val TEST_HIVE_DB: String = parseValue(app_config.getProperty("app.db.hive.test"))

  //impala url，根据当前操作系统选择参数值
  val IMPALA_URL: String =
    if(os.indexOf("linux") >= 0)
      parseValue(app_config.getProperty("app.impala.url.cluster"))
    else
      parseValue(app_config.getProperty("app.impala.url.local"))

  //邮件账号 密码 url
  val MAIL_USER: String = parseValue(app_config.getProperty("app.mail.user"))
  val MAIL_PASSWORD: String = parseValue(app_config.getProperty("app.mail.password"))
  val MAIL_URL: String = parseValue(app_config.getProperty("app.mail.url"))
  val MAIL_TO: String = parseValue(app_config.getProperty("app.mail.to"))

  //kudu master
  val KUDU_MASTER: String = parseValue(app_config.getProperty("app.kudu.master"))

  //hdfs url
  val HDFS_URL: String = parseValue(app_config.getProperty("app.hdfs.url"))

  /*
  Kerberos认证信息，根据当前操作系统选择参数值
  user.dir是yarn默认的用户目录，在spark提交时要把配置文件放在工作区里面，这样文件就会被自动上传到该目录
   */
  val KRB_USER: String = parseValue(app_config.getProperty("app.kerberos.user"))

  val KEYTAB_URL: String =
    if(os.indexOf("linux") >= 0)
      parseValue(app_config.getProperty("app.kerberos.keytab.cluster"))
    else
      parseValue(app_config.getProperty("app.kerberos.keytab.local"))

  val KRB5_URL: String =
    if(os.indexOf("linux") >= 0)
      parseValue(app_config.getProperty("app.kerberos.krb5.cluster"))
    else
      parseValue(app_config.getProperty("app.kerberos.krb5.local"))

  /*
  log4j.properties的路径
  user.dir是yarn默认的用户目录，在spark提交时要把配置文件放在工作区里面，这样文件就会被自动上传到该目录
   */
  val LOG4J_PATH: String =
    if(os.indexOf("linux") >= 0)
      parseValue(app_config.getProperty("app.log4j.cluster"))
    else
      parseValue(app_config.getProperty("app.log4j.local"))
}
