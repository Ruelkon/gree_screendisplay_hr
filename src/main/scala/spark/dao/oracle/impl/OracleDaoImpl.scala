package spark.dao.oracle.impl

import org.apache.spark.sql.SparkSession
import spark.dao.oracle.BaseOracleDao

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 14:18
 * Created by: 聂嘉良
 */
class OracleDaoImpl(_spark: SparkSession) extends BaseOracleDao {
  override var spark: SparkSession = _spark
}
