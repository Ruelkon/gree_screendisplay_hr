package spark.util

import java.sql.Timestamp
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import spark.util.DateUtil.getTodayDate
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
 * Author: 260371
 * Date: 2021/11/10
 * Time: 19:26
 * Created by: 聂嘉良
 */
class ActorUtil {

  //会自动加载class目录下的application.conf配置文件，若不想自动则需手动-Dconfig.file指定
  val actorFactory: ActorSystem = ActorSystem("actorFactory")
//  println(actorFactory.settings)

  /**
   * 提供了一种并发方法，把要并发执行的代码块传入方法，方法内会生成actor执行代码
   * @param sth 要并发执行的代码块
   */
  def doSth(sth: => Any): Unit ={
    val doSthActorRef: ActorRef = actorFactory.actorOf(Props(new DoSthActor(sth)) )
    doSthActorRef ! "start"
  }

  /**
   * 提供了一种并发执行并收集异常的方法
   * @param sth 要并发执行的代码块
   * @param thisErr 给该异常命名
   * @param errList 你自己提供的异常列表，并发出现的错误都会写到这个列表中，这个列表的写操作有同步锁控制
   */
  def doSthAndCollectException(sth: => Any, thisErr: String, errList: ListBuffer[String]): Unit ={
    val doSthActorRef: ActorRef = actorFactory.actorOf(Props(new DoSthActor(sth)))
    doSthActorRef ! ("start", thisErr, errList)
  }

  /**
   * 等待并发任务执行完成并释放资源
   * 在所有并发任务之后调用
   * @return
   */
  def close: Terminated = {
    actorFactory.terminate()
    Await.result(actorFactory.whenTerminated, Duration.Inf)
  }

  /**
   * 定时线程
   * @param sth 线程要执行的代码
   * @param scheduleTime 定时时间，格式：hh:mm:ss
   */
  def scheduler(sth: => Any, scheduleTime: String): Unit ={
    def getTimestamp(hour: Int, minute: Int, second: Int) = {
      val c = Calendar.getInstance()
      c.set(getTodayDate.substring(0,4).toInt, getTodayDate.substring(5,7).toInt-1, getTodayDate.substring(8,10).toInt, hour, minute, second)
      c.getTimeInMillis
    }

    val time = scheduleTime.split(":").map(_.toInt)
    val delay = getTimestamp(time(0), time(1), time(2)) - System.currentTimeMillis()
    actorFactory.scheduler.scheduleOnce(Duration.create(delay, TimeUnit.MILLISECONDS), new Runnable {
      override def run(): Unit = sth
    })(actorFactory.dispatcher)
  }
}

class DoSthActor(sth: => Any) extends Actor{
  var another: ActorRef = self

  override def receive: Receive = {
    case "start" =>
      another ! sth

    case ("start", thisErr: String, errList: ListBuffer[String]) =>
      another ! (Try(sth) match {
        case Success(_) =>
        case Failure(e) =>
          synchronized(errList.append(thisErr))
      })

    case "stop" =>
      context.stop(self)
  }
}
