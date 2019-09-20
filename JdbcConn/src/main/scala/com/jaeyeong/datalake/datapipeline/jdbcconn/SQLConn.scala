package com.jaeyeong.datalake.datapipeline.jdbcconn

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer

abstract class SQLConn(connType:String) {

  protected val username = "superuser"
  protected val password = "superuser@admin"

  protected[jdbcconn] val (driverName:String,connUrl:String) = {
    val rcConf = ConfigFactory.load("conn_info")

    val jdbcDriverName = rcConf.hasPath("jdbc_driver_name." + connType) match {
      case true => Some(rcConf.getString("jdbc_driver_name." + connType))
      case false => None
    }
    val jdbcConnUrl = rcConf.hasPath("jdbc_conn_url." + connType) match {
      case true => Some(rcConf.getString("jdbc_conn_url." + connType))
      case false => None
    }
    val jdbcAuthMech = rcConf.hasPath("jdbc_auth_mech." + connType) match {
      case true => rcConf.getString("jdbc_auth_mech." + connType)
      case false => "%s;%s"
    }

    val driverName = jdbcDriverName.getOrElse("")
    val connUrl= jdbcConnUrl.getOrElse("") + ";" + jdbcAuthMech.format(username,password)

    (driverName,connUrl)
  }

  override def toString: String = {
    val builder = StringBuilder.newBuilder
    builder.append("connUrl : " + connUrl + "\n")
    builder.append("driverName : " + driverName)
    builder.toString()
  }

  def realize(queryResult: ResultSet): Seq[Map[String, Object]] = {
    def buildMap(queryResult: ResultSet, colNames: Seq[String]): Option[Map[String, Object]] =
      if (queryResult.next())
        Some(colNames.map(n => n -> queryResult.getObject(n)).toMap)
      else
        None

    val md = queryResult.getMetaData
    val colNames = (1 to md.getColumnCount) map md.getColumnName
    Iterator.continually(buildMap(queryResult, colNames)).takeWhile(!_.isEmpty).map(_.get).toSeq
  }

  def resultSetToSeqMap(rs: ResultSet) = {
    val metaData = rs.getMetaData

    val colNames = (1 to metaData.getColumnCount) map rs.getMetaData.getColumnName
    val rows = new ListBuffer[scala.collection.mutable.Map[String,Object]]()
    while (rs.next()){
      val colMap = colNames.foldLeft(scala.collection.mutable.Map[String,Object]()){
        (retMap:scala.collection.mutable.Map[String,Object], colName:String) => {
          retMap += (colName -> rs.getString(colName))
          retMap
        }
      }
      rows += colMap
    }
    rows.toSeq
  }

  def execute(sqlStr:String)  = {
    var conn:Connection = null
    try {
      Class.forName(driverName)
      conn = DriverManager.getConnection(connUrl)
      val statement = conn.createStatement
      statement.execute(sqlStr)
    } catch {
      case e : SQLException => false
    } finally {
      if(conn!=null) conn.close()
    }
  }

  def executeQuery(sqlStr:String) : Option[Seq[scala.collection.mutable.Map[String, Object]]]  = {
    var conn:Connection = null
    try {
      Class.forName(driverName)
      conn = DriverManager.getConnection(connUrl)
      val statement = conn.createStatement
      val rs = statement.executeQuery(sqlStr)
      val res:Seq[scala.collection.mutable.Map[String, Object]] = resultSetToSeqMap(rs)
      Some(res)
    } catch {
      case e : SQLException => None
    } finally {
      if(conn!=null) conn.close()
    }
  }
}
