package spark.app.impl

import spark.app.BaseSparkApp
import spark.service.getHrDataAccordingToDate

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 17:04
 *
 * Created by: 林雄城
 */
class update50AndBaseAppImpl extends BaseSparkApp {
  /**
   * 接入人力的基本表的相关接口
   */
  override def onRun(): Unit = {
    val runSpark = spark
    new getHrDataAccordingToDate(runSpark).logic()
    println(System.getProperty("user.dir")+"---------")
  }
}

object update50AndBaseAppImpl{
  def main(args: Array[String]): Unit = {
    new update50AndBaseAppImpl().startApp()
  }
}
