package spark.exception.impl

import spark.exception.BaseExceptionHandler

/**
 * Author: 260371
 * Date: 2021/11/9
 * Time: 9:23
 * Created by: 聂嘉良
 */
class ExceptionHandlerImpl(_exception: Throwable) extends BaseExceptionHandler {

  override protected var exception: Throwable = _exception

}
