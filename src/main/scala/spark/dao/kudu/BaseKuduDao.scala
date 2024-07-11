package spark.dao.kudu

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.common.Constants
import spark.util.{DateUtil, KerberosUtil}

/**
 * Author: 260371
 * Date: 2021/9/10
 * Time: 10:01
 * Created by: 聂嘉良
 */
abstract class BaseKuduDao {

  var spark: SparkSession

  val logger: Logger = Logger.getLogger(getClass)

  def read(kuduMaster: String, database: String, kuduTable: String): DataFrame = {
    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    UserGroupInformation.getLoginUser.doAs(
      new PrivilegedExceptionAction[DataFrame] {
        override def run(): DataFrame =
          spark.read.format("kudu")
            .option("kudu.master", kuduMaster)
            .option("kudu.table", "impala::" + database + "." + kuduTable)
            .load()
      }
    )
  }

  def write(database: String, tablename: String, df: DataFrame, saveMode: String): Unit = {
    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    val kuduContext = UserGroupInformation.getLoginUser.doAs(
      new PrivilegedExceptionAction[KuduContext] {
        override def run(): KuduContext = new KuduContext(Constants.KUDU_MASTER, spark.sparkContext)
      }
    )
    if ("overwrite".equals(saveMode)) {
      val targetDF = spark.read.format("kudu")
        .option("kudu.master", Constants.KUDU_MASTER)
        .option("kudu.table", "impala::" + database + "." + tablename).load()
      kuduContext.deleteRows(targetDF, "impala::" + database + "." + tablename)
      kuduContext.insertRows(df, "impala::" + database + "." + tablename)
    }
    if ("append".equals(saveMode)) {
      kuduContext.upsertRows(df, "impala::" + database + "." + tablename)
    }
    logger.info("【" + database + "." + tablename + "】->Kudu at " + DateUtil.getTimeNow + "保存完成")
  }
}
