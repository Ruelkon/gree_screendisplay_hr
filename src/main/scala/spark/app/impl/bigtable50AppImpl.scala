package spark.app.impl

import spark.app.BaseSparkApp
import spark.service.insertIntoHrBigTable

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 17:04
 *
 * Created by: 林雄城
 */
class bigtable50AppImpl extends BaseSparkApp {
  /**
   * 根据已有的表和接口接入的表，合并成人力大宽表gree_screendisplay_hr.hr_bigtable_dayrecord(会更新最近的50天的数据)
   */
  override def onRun(): Unit = {
    val runSpark = spark
    new insertIntoHrBigTable(runSpark).logic()
    println(System.getProperty("user.dir")+"---------")
  }
}

object bigtable50AppImpl{
  def main(args: Array[String]): Unit = {
    new bigtable50AppImpl().startApp()
  }
}
