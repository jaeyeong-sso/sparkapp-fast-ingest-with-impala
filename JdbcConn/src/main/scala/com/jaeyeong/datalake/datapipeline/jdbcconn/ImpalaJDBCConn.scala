package com.jaeyeong.datalake.datapipeline.jdbcconn

class ImpalaJDBCConn extends SQLConn("impala") {

  override def toString: String = super.toString
}

object ImpalaJDBCConn {

  private val instance = new ImpalaJDBCConn
  val debugClassObj = instance.toString

  // Dummy code to guarantee shaded.jar could be considered the dynamic loaded class regarding JDBC driver.
  def dummyForMavenShadedPluginMinimaizeJar(): Unit ={
    val driver = new com.cloudera.impala.jdbc41.Driver
  }

  def execute(sql:String)  = {
    instance.execute(sql)
  }

  def executeQuery(sql:String)  = {
    instance.executeQuery(sql)
  }
}
