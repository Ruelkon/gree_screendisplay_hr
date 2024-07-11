package spark.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.common.config.TableConfig
import scala.collection.mutable.ListBuffer

/**
 * Author: 260371
 * Date: 2021/1/27
 * Time: 12:22
 * Created by: 聂嘉良
 */
object DataFrameUtil {
  /**
   * 将两个DataFrame的schema统一，思路如下：
   * 1. 列名不同，直接withColumn添加没有的列；
   * 2. 列名相同类型不同，若可以union时自动转换(int和long)，则不操作；若无法自动转换(long和timestamp)，则使用withColumn手动转换；
   */
  def makeSchemaSame(df1: DataFrame, df2: DataFrame, spark: SparkSession): (DataFrame, DataFrame) = {
    var res_1: DataFrame = makeSchemaLower(df1, spark)
    var res_2: DataFrame = makeSchemaLower(df2, spark)
    val schema1 = res_1.schema
    val schema2 = res_2.schema
    val diff_1_2 = schema1.diff(schema2) :+ StructField("", StringType)
    val diff_2_1 = schema2.diff(schema1) :+ StructField("", StringType)
    diff_1_2.foreach(col_1_2 => {
      diff_2_1.foreach(col_2_1 => {
        if (col_2_1.name == col_1_2.name) {
          (col_1_2.dataType, col_2_1.dataType) match {
            case (LongType, TimestampType) =>
              res_1 = res_1.withColumn(col_2_1.name, to_utc_timestamp(from_unixtime(col(col_2_1.name).substr(1, 10)), "UTC+8"))
            case (TimestampType, LongType) =>
              res_2 = res_2.withColumn(col_1_2.name, to_utc_timestamp(from_unixtime(col(col_1_2.name).substr(1, 10)), "UTC+8"))
            case _ =>
          }
        }
        else {
          if (!diff_1_2.map(_.name).contains(col_2_1.name)) res_1 = res_1.withColumn(col_2_1.name, lit(null).cast(col_2_1.dataType))
          if (!diff_2_1.map(_.name).contains(col_1_2.name)) res_2 = res_2.withColumn(col_1_2.name, lit(null).cast(col_1_2.dataType))
        }
      })
    })
    (res_1, res_2)
  }


  /**
   * 先使两个DataFrame的schema相同，再union
   *
   * @param df1
   * @param df2
   * @return
   */
  def safeUnion(df1: DataFrame, df2: DataFrame, spark: SparkSession): DataFrame = {
    val tp = makeSchemaSame(df1, df2, spark)
    tp._1 unionByName tp._2
  }

  /**
   * 将列名转小写
   */
  def makeSchemaLower(df: DataFrame, spark: SparkSession): DataFrame = {
    val schema = df.schema
    val data = df.rdd

    spark.createDataFrame(data, StructType(schema.map(sf => StructField(sf.name.toLowerCase, sf.dataType, sf.nullable, sf.metadata))))
  }

  def cleanSchema(df: DataFrame, spark: SparkSession): DataFrame = {
    val schema = df.schema
    val data = df.rdd

    spark.createDataFrame(data, StructType(schema.map(sf => StructField(sf.name.replaceAll("[,;{}()\n\t\r]", ""), sf.dataType, sf.nullable, sf.metadata))))
  }

  def setIncreasedId(df: DataFrame, idName: String, spark: SparkSession): DataFrame = {
    val schema = df.schema.add(idName, LongType)
    val data = df.rdd.zipWithIndex().map {
      case (row, index) => Row(row.toSeq :+ (index + 1): _*)
    }
    spark.createDataFrame(data, schema)
  }

  /**
   * 将类型字符串转为类型，如 "string" -> StringType，"timestamp" -> TimestampType
   * @param name
   * @return
   */
  def typeNameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    val nonDecimalNameToType = {
      Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
        DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
        .map(t => t.typeName -> t).toMap
    }
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType.getOrElse(
        other,
        throw new IllegalArgumentException(
          s"Failed to convert the JSON string '$name' to a data type."))
    }
  }

  def cleanDataFrame(df: DataFrame, targetSchema: StructType=null, spark: SparkSession): DataFrame = {
    /**
     * 字段名清洗：带保留字的如t$date改为t_date, 不带保留字的去掉t$, 其余的$替换为_
     */
    val schema = StructType(df.schema.map(struct_field =>
      StructField(struct_field.name.toLowerCase.replaceAll("[t]\\$(date|full|conf|user)", "t_$1")
        .replaceAll("[t]\\$", "")
        .replaceAll("\\$", "_"),
        struct_field.dataType,
        struct_field.nullable,
        struct_field.metadata)))

    //数据清洗：去掉左右空格，将\n、\r、\t替换为空格，"null"替换为null
    val rdd: RDD[Row] = df.rdd.map(row => {
      val field_iterator = schema.iterator
      row.toSeq.map((_, field_iterator.next().dataType))
    })
      .map(seq => seq.map(tuple => if (tuple._2==StringType) String.valueOf(tuple._1).replaceAll("[\n\r\t]", " ").trim else tuple._1))
      .map(seq => Row(seq.map(data => if(data == "null") null else data):_*))

    var result = spark.createDataFrame(rdd, schema)

    //数据类型清洗：若targetSchema不为空，则按照targetSchema转换，否则按照配置文件转换
    if(targetSchema == null){
      schema.foreach(struct_field => {
        result = result
          .withColumn(
            struct_field.name,
            col(struct_field.name)
              .cast(
                TableConfig.type_cast_map.getOrElse(
                  if(struct_field.dataType.typeName.matches(".*decimal.*"))
                    "decimal"
                  else
                    struct_field.dataType.typeName,
                  struct_field.dataType.typeName
                )
              )
          )
      })
    }else{
      targetSchema.foreach(struct_field => {
        result = result.withColumn(struct_field.name, col(struct_field.name).cast(struct_field.dataType))
      })
    }

    result
  }

}
