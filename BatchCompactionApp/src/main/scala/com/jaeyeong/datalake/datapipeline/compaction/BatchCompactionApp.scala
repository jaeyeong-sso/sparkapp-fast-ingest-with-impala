package com.jaeyeong.datalake.datapipeline.compaction

import com.jaeyeong.datalake.datapipeline.jdbcconn.{HiveJDBCConn, ImpalaJDBCConn}
import com.jaeyeong.datalake.datapipeline.lock.ZKDistLock
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.jaeyeong.datalake.datapipeline.schema.{HiveMetaStrategy, SchemaLoader}

object BatchCompactionApp extends App {

  val (
    dbName:String,
    logicalTblName:String,
    iRepartitionFactor:Int
    ) = OptionArgsParser.parseOptionArgs(this.args)

  val sparkSession = SparkSession.builder()
      .appName("BatchCompactionApp")
      .getOrCreate()
  val sqlctx = sparkSession.sqlContext

  doProcessPerTable(dbName,logicalTblName)


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  private def doProcessPerTable(dbName:String,tblName:String) = {
    if(switchActiveWriteTbl(dbName,tblName)){
      copyDataFromActiveReadTblToStandbyReadTbl(dbName,tblName)
      swapReadTblsAndTruncateStandbyWriteTbl(dbName,tblName)
    }
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private[compaction] def switchActiveWriteTbl(dbName:String,logicalTblName:String) : Boolean = {

    val trLockNode = ZKDistLock.acquireLogTypeLock(dbName,logicalTblName)

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // [START] - Critical section
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val activeReadTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,logicalTblName,"read/active")
    val activeWriteTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,logicalTblName,"write/active")
    val standbyWriteTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,logicalTblName,"write/standby")

    val bIsValidData = if(
        (activeReadTbl._1.getOrElse(null)!= null) &&
        (activeWriteTbl._1.getOrElse(null)!= null) &&
        (activeWriteTbl._1.getOrElse(null)!= null)) {
          true
        } else {
          false
        }

    if (bIsValidData) {

      // (1). Modify(Extend) view definition including standby write tbl.
      extendViewToActiveReadActiveWriteStandbyWrite(
        activeReadTbl._1.getOrElse(""),
        activeWriteTbl._1.getOrElse(""),
        standbyWriteTbl._1.getOrElse("")
      )

      // (2). Swap Active/Standby Write Table
      ZKDistLock.switchActiveStandbyTbls(dbName, logicalTblName,"write")
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // [END] - Critical section
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ZKDistLock.releaseLock(trLockNode)

    bIsValidData
  }

  private[compaction] def copyDataFromActiveReadTblToStandbyReadTbl(dbName:String,tblName:String) : Unit = {

    val standbyReadTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"read/standby")
    val standbyWriteTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"write/standby")

    val activeWriteTblDf = sqlctx.read.parquet(standbyWriteTbl._2.get).drop("storeid")
    activeWriteTblDf.repartition(iRepartitionFactor).write.partitionBy("dt","appid").
      format("parquet").
      mode(SaveMode.Append).
      save(standbyReadTbl._2.get)

    HiveJDBCConn.execute("MSCK REPAIR TABLE " + dbName + "." + standbyReadTbl._1.get)
    ImpalaJDBCConn.execute("REFRESH " + dbName + "." + standbyReadTbl._1.get)
    ImpalaJDBCConn.execute("COMPUTE STATS " + dbName + "." + standbyReadTbl._1.get)
  }

  private[compaction] def swapReadTblsAndTruncateStandbyWriteTbl(dbName:String,tblName:String) : Unit = {

    val activeReadTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"read/active")
    val standbyReadTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"read/standby")
    val activeWriteTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"write/active")
    val standbyWriteTbl = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"write/standby")

    // (1). Modify(Shrink) view definition excluding standby write tbl.
    shrinkViewToStandbyReadActiveWrite(
        standbyReadTbl._1.getOrElse(""), activeWriteTbl._1.getOrElse("")
    )

    // (2). Swap Active/Standby Read Table & Make swapped-standby read tbl aware changes.
    ZKDistLock.switchActiveStandbyTbls(dbName, tblName,"read")

    HiveJDBCConn.execute("MSCK REPAIR TABLE " + dbName + "." + standbyReadTbl._1.get)   // or ALTER TABLE ADD PARTITION
    ImpalaJDBCConn.execute("REFRESH " + dbName + "." + standbyReadTbl._1.get)
    ImpalaJDBCConn.execute("COMPUTE STATS " + dbName + "." + standbyReadTbl._1.get)

    // (3). Prepare next iteration(truncate standby write tbl)
    // DROP & CREATE STANDBY-WRITE-TBL considering TBL schema(via DESCRIBE ACTIVE-WRITE-TBL result)
    recreateHiveTbl(dbName, standbyWriteTbl._1.get)

    // (4). Aware the un-linked read-tbl.
    HiveJDBCConn.execute("MSCK REPAIR TABLE " + dbName + "." + activeReadTbl._1.get)    // or ALTER TABLE ADD PARTITION
    ImpalaJDBCConn.execute("REFRESH " + dbName + "." + activeReadTbl._1.get)
  }

  private[compaction] def extendViewToActiveReadActiveWriteStandbyWrite (
        tblActiveRead:String, tblActiveWrite:String, tblStandbyWrite:String ) = {

    val schemaLoader = new SchemaLoader(dbName,tblActiveRead) with HiveMetaStrategy
    val strFields = schemaLoader.listColSchema.map(
      mapNameType => mapNameType.getOrElse(schemaLoader.MAP_KEY_NAME, "")
    ).mkString(",")

    val strSql = ("ALTER VIEW %s.v_%s AS SELECT %s FROM %s.%s UNION ALL SELECT %s FROM %s.%s UNION ALL SELECT %s FROM %s.%s").format(
      dbName,logicalTblName,
      strFields,dbName,tblActiveRead,
      strFields,dbName,tblActiveWrite,
      strFields,dbName,tblStandbyWrite
    )
    ImpalaJDBCConn.execute(strSql)
  }

  private[compaction] def shrinkViewToStandbyReadActiveWrite (
        tblStandbyRead:String, tblActiveWrite:String ) ={

    val schemaLoader = new SchemaLoader(dbName,tblStandbyRead) with HiveMetaStrategy
    val strFields = schemaLoader.listColSchema.map(
      mapNameType => mapNameType.getOrElse(schemaLoader.MAP_KEY_NAME, "")
    ).mkString(",")

    val strSql = ("ALTER VIEW %s.v_%s AS SELECT %s FROM %s.%s UNION ALL SELECT %s FROM %s.%s").format(
      dbName,logicalTblName,
      strFields,dbName,tblStandbyRead,
      strFields,dbName,tblActiveWrite
    )
    ImpalaJDBCConn.execute(strSql)
  }

  private[compaction] def recreateHiveTbl(dbName:String,tblName:String) = {

    val schemaLoader = new SchemaLoader(dbName,tblName) with HiveMetaStrategy

    HiveJDBCConn.execute("DROP TABLE " + dbName +"." + tblName)

    val strFields = schemaLoader.listColSchema.map(
      mapNameType => {
        val strName = mapNameType.getOrElse(schemaLoader.MAP_KEY_NAME, "")
        val strType = mapNameType.getOrElse(schemaLoader.MAP_KEY_TYPE,"")
        val strNameType = if (!(strName.isEmpty || strType.isEmpty)) strName+" "+strType else ""
        strNameType
      }
    ).mkString(",")

    val strParts = schemaLoader.listPartSchema.map(
      mapNameType => {
        val strName = mapNameType.getOrElse(schemaLoader.MAP_KEY_NAME, "")
        val strType = mapNameType.getOrElse(schemaLoader.MAP_KEY_TYPE,"")
        val strNameType = if (!(strName.isEmpty || strType.isEmpty)) strName+" " +strType else ""
        strNameType
      }
    ).mkString(",")

    val strTblLoc = schemaLoader.listLocSchema.head.getOrElse(schemaLoader.MAP_KEY_TYPE,"")

    val strSqlCreateTbl = ("CREATE EXTERNAL TABLE %s.%s (%s) PARTITIONED BY (%s) STORED AS PARQUET LOCATION '%s'").format(dbName,tblName,strFields,strParts,strTblLoc)

    HiveJDBCConn.executeQuery(strSqlCreateTbl)
  }
}
