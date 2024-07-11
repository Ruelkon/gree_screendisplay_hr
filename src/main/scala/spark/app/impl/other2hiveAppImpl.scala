package spark.app.impl

import spark.app.BaseSparkApp
import spark.service.getHrDataToOther

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 17:04
 *
 * Created by: 林雄城
 */
class other2hiveAppImpl extends BaseSparkApp {
  /**
   * 用于后期添加的接口数据接入
   */
  override def onRun(): Unit = {
    val runSpark = spark
    new getHrDataToOther(runSpark).logic()
    println(System.getProperty("user.dir")+"---------")
  }
}

object other2hiveAppImpl {
  def main(args: Array[String]): Unit = {
    new other2hiveAppImpl().startApp()
  }
}
