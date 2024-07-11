package spark.exception

import org.apache.log4j.Logger
import spark.common.Constants
import spark.util.MailUtil

/**
 * Author: 260371
 * Date: 2021/11/9
 * Time: 9:19
 * Created by: 聂嘉良
 */
abstract class BaseExceptionHandler extends ExceptionHandlerTrait {

  protected var exception: Throwable

  val logger: Logger = Logger.getLogger(getClass)

  override def onException(): Unit = {}

  /**
   * 对传入的异常记录到日志，并以邮件形式发送
   */
  override def onException(exceptionInfo: String): Unit = {
    var info = exceptionInfo + "<br/>"

    exception.getStackTrace.foreach(ex => info += ("<br/>" + ex))

    logger.error(info)

    new MailUtil(Constants.MAIL_URL).send(Constants.MAIL_USER, Constants.MAIL_PASSWORD, Constants.MAIL_TO, "Spark程序运行发生异常！", info, "", "")
  }
}
