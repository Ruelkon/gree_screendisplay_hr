package spark.common.config

/**
 * Author: 260371
 * Date: 2021/11/22
 * Time: 19:22
 * Created by: 聂嘉良
 */
object TableConfig extends ConfigTrait {
  //"full"/"fullpart"/"delta"...，更新模式
  val update_type: String = data_config \ "table" \ "update_type" text

  //"T0"/"T1" 增量模式
  val delta_mode: String = data_config \ "table" \ "delta_mode" text

  if((delta_mode != "T0") && (delta_mode != "T1") && (delta_mode != ""))
    throw new IllegalArgumentException("参数delta_mode不正确，请检查对应参数！")

  //Map("timestamp"->"string","date"->"string",...)，数据类型转换map
  val type_cast_map: Map[String, String] = (data_config \ "table" \ "type_cast" text).split(";").map(src_target => {
    (src_target.split("-")(0),src_target.split("-")(1))
  }).toMap

  //oracle/sqlserver/mysql，数据源类型
  val source_type: String = data_config \ "table" \ "source" \ "type" text

  //数据源jdbc连接信息
  val source_jdbc_url: String = data_config \ "table" \ "source" \ "url" text
  val source_jdbc_user: String = data_config \ "table" \ "source" \ "user" text
  val source_jdbc_password: String = data_config \ "table" \ "source" \ "password" text

  //数据源表所在的数据库名
  val source_db: String = data_config \ "table" \ "source" \ "db" text

  //Array("table1", "table2",...)，数据源表名数组
  val source_table_names: Array[String] = (data_config \ "table" \ "source" \ "table_name" text).split(";")

  //Array("col1","col2","col3-col4",...)，数据源表增量字段数组
  val source_delta_fields: Array[String] = (data_config \ "table" \ "source" \ "delta_field" text).split(";")

  //hive/kudu，目标表类型
  val target_type: String = data_config \ "table" \ "target" \ "type" text

  //目标表库名
  val target_db: String = data_config \ "table" \ "target" \ "db" text

  //Array("table1", "table2",...)，数据目标表名数组
  val target_table_names: Array[String] = (data_config \ "table" \ "target" \ "table_name" text).split(";")

  //Array("col1","col2","col3-col4",...)，数据目标表增量字段数组
  val target_delta_fields: Array[String] = (data_config \ "table" \ "target" \ "delta_field" text).split(";")

  //Array("col1-month","col2-month","col3-day",...)，目标表分区字段数组
  val target_partition_fields: Array[String] = (data_config \ "table" \ "target" \ "partition_field" text).split(";")

  (target_type, target_partition_fields) match {
    case ("kudu", fields) if fields.length > 1 => throw new IllegalArgumentException("update_type为kudu时不应有分区字段，请检查对应参数！")
    case _ =>
  }

  if(((update_type == "delta") || (update_type == "deltapart")) && (source_table_names.length != source_delta_fields.length)){
    throw new IllegalArgumentException("源表名【table.source.table_name】 和 源表增量字段【table.source.delta_field】 参数长度不同，请检查对应参数！")
  }
  if(source_table_names.length != target_table_names.length){
    throw new IllegalArgumentException("源表名【table.source.table_name】 和 目标表名【table.target.table_name】 参数长度不同，请检查对应参数！")
  }
  if(((update_type == "delta") || (update_type == "deltapart")) && (source_table_names.length != target_delta_fields.length)){
    throw new IllegalArgumentException("源表名【table.source.table_name】 和 目标表增量字段【table.target.delta_field】 参数长度不同，请检查对应参数！")
  }
  if((update_type == "deltapart") && (source_table_names.length != target_partition_fields.length)){
    throw new IllegalArgumentException("源表名【table.source.table_name】 和 目标表分区字段【table.target.partition_field】 参数长度不同，请检查对应参数！")
  }

  val tableArgs: Array[(String, String, String, String, Any, Any, String, String, Any, Any, String)] = source_table_names.zip(target_table_names)
    .zipAll(source_delta_fields, "", "")
    .zipAll(target_delta_fields, "", "")
    .zipAll(target_partition_fields, "", "")
    .map{
      case ((((source_table_name, target_table_name), source_delta_field), target_delta_field), target_partition_field) =>
        ( source_type,
          source_jdbc_url,
          source_jdbc_user,
          source_jdbc_password,
          source_table_name,
          source_delta_field,
          target_type,
          target_db,
          target_table_name,
          target_delta_field,
          target_partition_field)
    }

}
