package spark.dao.mysql.impl

import org.apache.spark.sql.SparkSession
import spark.dao.mysql.BaseMySqlDao

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 14:12
 * Created by: 聂嘉良
 */
class MySqlDaoImpl(_spark: SparkSession) extends BaseMySqlDao {
  override var spark: SparkSession = _spark
}
