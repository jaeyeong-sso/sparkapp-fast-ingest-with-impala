package com.jaeyeong.datalake.datapipeline.jdbcconn

import java.sql.DriverManager

import com.cloudera.impala.impala.core.ImpalaJDBC
import org.apache.hive.jdbc.HiveDriver
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SQLConnTest extends FlatSpec with Matchers {

  "ImpalaJDBCConn" should "inherit from Impala resources config." ignore {
    println(ImpalaJDBCConn.debugClassObj)
    ImpalaJDBCConn.debugClassObj.contains(":21051") should be (true)
  }

  "HiveJDBCConn" should "inherit from Hive resources config." ignore {
    println(HiveJDBCConn.debugClassObj)
    HiveJDBCConn.debugClassObj.contains(":10000") should be (true)
  }

  "[HiveJDBCConn] Non-result execute function" should "should be return FALSE always" ignore {
    val bRet  = HiveJDBCConn.execute("MSCK REPAIR TABLE inhouse_common.hbaccess_read_a")
    bRet should be (false)
  }

  "[HiveJDBCConn] Result executeQuery function" should "should return table described information rows." ignore {
    val res  = HiveJDBCConn.executeQuery("DESCRIBE FORMATTED inhouse_common.hbaccess_read_a")
    val mapSeq : Seq[mutable.Map[String,Object]] = res.getOrElse(null)

    if(mapSeq!=null){
      mapSeq.foreach(
        map => {
          map.keySet.foreach(
            mapKey => println(mapKey.getBytes + " : " + map.get(mapKey))
          )
        }
      )
    }
    res.getOrElse(Nil).length should be > (0)
  }

  "[ImpalaJDBCConn] Non-result execute function" should "should be return FALSE always" ignore {
    val bRet  = ImpalaJDBCConn.execute("INVALIDATE METADATA")
    bRet should be (false)
  }

  "[ImpalaJDBCConn] Result executeQuery function" should "should return table described information rows." ignore {
    val res  = ImpalaJDBCConn.executeQuery("DESCRIBE FORMATTED inhouse_common.hbaccess_read_a")
    res.foreach(println)
    res.getOrElse(Nil).length should be > (0)
  }

}
