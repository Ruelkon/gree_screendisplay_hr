package spark.exception

/**
 * Author: 260371
 * Date: 2021/11/9
 * Time: 9:16
 * Created by: 聂嘉良
 */

/**
 * 异常处理器特质
 */
trait ExceptionHandlerTrait {

  /**
   * 异常处理方法
   */
  protected def onException(): Unit

  /**
   * 带自定义信息的异常处理方法
   * @param exceptionInfo 自定义的异常信息
   */
  protected def onException(exceptionInfo: String): Unit

}
