package spark.service

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{column, upper}
import org.spark_project.guava.collect.ImmutableMap
import spark.dao.hive.impl.HiveDaoImpl

import scala.Console.println

//实现相关表的数据清洗功能,目前包括hr_card_record,hr_bigtable_day_record
class dataEtl(spark: SparkSession) {
  val logger: Logger = Logger.getLogger(getClass)
  val sparkSession = new HiveDaoImpl(spark)
  def logic(): Unit = {
//    new KerberosUtil().krbLoginedUGI(Constants.KRB_USER, Constants.KEYTAB_URL, Constants.KRB5_URL)
    val start = System.currentTimeMillis()
    parallelInsert()
    val end = System.currentTimeMillis()
    println("花费了"+(end - start)+"ms")
    println("查询成功")
  }
  //并行写入
  def parallelInsert(): Unit = {
    //打卡记录表清洗
     var cardRecordDF = spark.sql("select * from gree_screendisplay_hr.hr_card_record")
     cardRecordDF.show()
//    //数据清洗：bigDataframe包含shiftcode列的（同时几个表用该方法接入，不同的表字段不同，需要判断一下），将小写字母转成大写，并且去除后面的空格
//    cardRecordDF = cardRecordDF.withColumn("shiftcode",upper(column("shiftcode")))
//        //去除字符末尾的空格
//        .na.replace("shiftcode",ImmutableMap.of("NORM ","NORM"))
//    sparkSession.write("gree_screendisplay_hr", "hr_card_record_bak", cardRecordDF, SaveMode.Append,Array("signdate"))
  }
}
