package spark.dao.hive.impl

import org.apache.spark.sql.SparkSession
import spark.dao.hive.BaseHiveDao

/**
 * Author: 260371
 * Date: 2021/8/5
 * Time: 14:39
 * Created by: 聂嘉良
 */
class HiveDaoImpl(_spark: SparkSession) extends BaseHiveDao {
  override var spark: SparkSession = _spark
}
