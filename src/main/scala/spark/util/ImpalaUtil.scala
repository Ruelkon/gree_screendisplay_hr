package spark.util

import java.sql.DriverManager

class ImpalaUtil(impalaUrl: String) {


  private def executeImpalaQuery(sql: String): Unit = {
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    val conn = DriverManager.getConnection(impalaUrl)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    println("begin query")
    while(rs.next()){
      println(rs.getString(1))
    }
    println("end query")
  }
  private def executeImpala(sql: String) = {
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    val conn = DriverManager.getConnection(impalaUrl)
    val stmt = conn.createStatement()
    stmt.execute (sql)
  }
  def invalidateTable(tableName: String): Unit ={
    executeImpala("invalidate metadata " + tableName + ";")
  }

}
