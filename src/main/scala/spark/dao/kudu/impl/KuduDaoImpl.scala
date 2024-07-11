package spark.dao.kudu.impl

import org.apache.spark.sql.SparkSession
import spark.dao.kudu.BaseKuduDao

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 10:46
 * Created by: 聂嘉良
 */

/**
 * 实现类样例
 */
class KuduDaoImpl(_spark: SparkSession) extends BaseKuduDao {
  override var spark: SparkSession = _spark
}
