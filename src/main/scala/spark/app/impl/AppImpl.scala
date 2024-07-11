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
class AppImpl extends BaseSparkApp {
  /**
   * 测试使用
   * 在需要时单独跑需求代码，正式代码不跑该代码
   */
  override def onRun(): Unit = {
    val runSpark = spark
//    new MyLogic(runSpark).logic()
//    new getHrDataAccordingToEmp(runSpark).logic() //初始化empinfo表
//    new getHrDataAccordingToDate(runSpark).logic()
//    new getHrDataAccordingToCardRecord(runSpark).logic()
    new insertIntoHrBigTable(runSpark).logic()
//    new insertIntoHrBigTableEmpHistory(runSpark).logic()
//    new getHrDataAccordingToDayAttendance(runSpark).logic()
//    new getHrDataAccordingToDept(runSpark).logic()
//    new getHrDataToOther(runSpark).logic()
//    new dataEtl(runSpark).logic()
//    new updateBigTable(runSpark).logic()


    println(System.getProperty("user.dir")+"---------")

  }
}

object AppImpl {
  def main(args: Array[String]): Unit = {
    new AppImpl().startApp()
  }
}
