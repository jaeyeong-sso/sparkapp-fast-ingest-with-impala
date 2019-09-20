package com.jaeyeong.datalake.datapipeline.jdbcconn

class HiveJDBCConn extends SQLConn("hive") {

  override def toString: String = super.toString
}

object HiveJDBCConn {

  private val instance = new HiveJDBCConn
  val debugClassObj = instance.toString

  // Dummy code to guarantee shaded.jar could be considered the dynamic loaded class regarding JDBC driver.
  def dummyForMavenShadedPluginMinimaizeJar(): Unit ={
    val driver = new org.apache.hive.jdbc.HiveDriver
  }

  def execute(sql:String)  = {
    instance.execute(sql)
  }

  def executeQuery(sql:String)  = {
    instance.executeQuery(sql)
  }
}
