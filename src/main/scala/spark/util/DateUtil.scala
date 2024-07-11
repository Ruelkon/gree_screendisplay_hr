package spark.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtil {
  private val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  def getTodayDate: String = DATE_FORMAT.format(new Date())

  def getTimeNow: String = TIME_FORMAT.format(new Date())

  def getHourNow: String = HOUR_FORMAT.format(new Date())

  def getTimeStampNow: Timestamp = new Timestamp(new Date().getTime)

  def getYesterdayDate:String = {
    val c = Calendar.getInstance()
    c.setTime(new Date())
    c.add(Calendar.DATE, -1)
    DATE_FORMAT.format(c.getTime)
  }

  def getTomorrowDate:String = {
    val c = Calendar.getInstance()
    c.setTime(new Date())
    c.add(Calendar.DATE, 1)
    DATE_FORMAT.format(c.getTime)
  }

  def getNextDayDate(year:Int, month:Int, day:Int): String = {
    val c = Calendar.getInstance()
    c.set(year, month-1, day)
    c.add(Calendar.DATE, 1)
    DATE_FORMAT.format(c.getTime)
  }

  def unixToTime(unix_time: String): String = {
    if(unix_time == "")
      "null"
    else
      TIME_FORMAT.format(new Date(unix_time.toLong*1000))
  }

  def timeDiff(time1:Timestamp, time2:Timestamp): Long = {
    val c1 = Calendar.getInstance()
    val c2 = Calendar.getInstance()
    c1.setTime(time1)
    c2.setTime(time2)
    c1.getTimeInMillis - c2.getTimeInMillis
  }

  def getTodayTime(hour:Int, minut:Int, second:Int): Timestamp = {
    val c = Calendar.getInstance()
    c.set(getTodayDate.substring(0,4).toInt, getTodayDate.substring(5,7).toInt-1, getTodayDate.substring(8,10).toInt, hour, minut, second)
    new Timestamp(c.getTimeInMillis)
  }

  def dateAdd(date: String, add_days: Int): String = {
    val c = Calendar.getInstance()
    c.setTime(new Date())
    c.set(date.substring(0, 4).toInt,date.substring(5, 7).toInt - 1,date.substring(8, 10).toInt)
    c.add(Calendar.DATE, add_days)
    DATE_FORMAT.format(c.getTime)
  }

  def main(args: Array[String]): Unit = {
    println(unixToTime("1599846111"))
  }
}
