package spark.dao.sqlserver.impl

import org.apache.spark.sql.SparkSession
import spark.dao.sqlserver.BaseSqlServerDao

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 11:41
 * Created by: 聂嘉良
 */
class SqlServerDaoImpl(_spark: SparkSession) extends BaseSqlServerDao {
  override var spark: SparkSession = _spark
}
