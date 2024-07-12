package spark.app.impl

import spark.app.BaseSparkApp
import spark.service.{getHrDataAccordingToCardRecord, insertIntoHrBigTable}

/**
 * Author: 260371
 * Date: 2021/11/8
 * Time: 17:04
 *
 * Created by: 林雄城
 */
class updateCardRecord extends BaseSparkApp {
  /**
   * 更新打卡记录表，每半个小时更新一次
   */
  override def onRun(): Unit = {
    val runSpark = spark
    new getHrDataAccordingToCardRecord(runSpark).logic()
  }
}


