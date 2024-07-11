package spark.log

/**
 * Author: 260371
 * Date: 2021/11/24
 * Time: 14:03
 * Created by: 聂嘉良
 */

/**
 * 日志处理特质
 */
trait LogHandlerTrait {

  /**
   * 读取log4j.properties配置文件
   */
  protected def loadProperties(): Unit

  /**
   * 处理日志文件
   */
  protected def handleLog(): Unit
}
