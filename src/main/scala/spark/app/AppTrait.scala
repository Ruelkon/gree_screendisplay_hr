package spark.app

/**
 * Author: 260371
 * Date: 2021/11/5
 * Time: 15:33
 * Created by: 聂嘉良
 */
trait AppTrait {

  /**
   * 初始化，应用运行前
   */
  protected def onInit(): Unit

  /**
   * 应用开始运行
   */
  protected def onRun(): Unit

  /**
   * 应用结束
   */
  protected def onStop(): Unit

  /**
   * 应用销毁后调用
   */
  protected def onDestroyed(): Unit

}
