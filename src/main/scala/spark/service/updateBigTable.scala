package spark.service

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import spark.common.Constants
import spark.util.KerberosUtil

/**
 * @author daxiongmiao
 * @date 2023年07月25日 9:39
 * @package hr_analysis
 */
class updateBigTable(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass)
  def logic(): Unit = {
    println("dfs")
    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    var cardRecordDF = spark.sql("select * from gree_screendisplay_hr.hr_card_record")
    cardRecordDF.show(10)
  }
}
