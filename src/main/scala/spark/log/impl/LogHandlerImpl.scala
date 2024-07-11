package spark.log.impl

import org.apache.spark.sql.SparkSession
import spark.log.BaseLogHandler

/**
 * Author: 260371
 * Date: 2021/11/24
 * Time: 15:10
 * Created by: 聂嘉良
 */
class LogHandlerImpl(_spark: SparkSession) extends BaseLogHandler{
  override var spark: SparkSession = _spark
}
