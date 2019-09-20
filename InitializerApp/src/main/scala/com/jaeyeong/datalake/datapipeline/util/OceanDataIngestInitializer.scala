package com.jaeyeong.datalake.datapipeline.util

import com.jaeyeong.datalake.lock.ZKDistLock
import com.typesafe.config.ConfigFactory

object OceanDataIngestInitializer extends App {

  initializePerTable("inhouse_common","hbaccess")

  protected[util]  def initializePerTable(dbName:String,tblName:String) = {
    initializeZNodeHierarchy(dbName,tblName)
    initializeZNodeData(dbName,tblName)
  }

  protected[util]  def initializeZNodeHierarchy(dbName:String,tblName:String): Unit = {

    val zkAppRootPathPrefix = "/" + ZKDistLock.getZnAppRoot_0D + "/"

    // [0-Depth] Create Application Root ZNode
    ZKDistLock.createZNode("/" + ZKDistLock.getZnAppRoot_0D)

    // [1-Depth] Create ${DB_ROOT}/${TABLE_ROOT} ZNode
    ZKDistLock.createZNode(zkAppRootPathPrefix + dbName)
    ZKDistLock.createZNode(zkAppRootPathPrefix + dbName + "/" + tblName)
    val zkTablePathPrefix = "/" + ZKDistLock.getZnAppRoot_0D + "/" + dbName + "/" + tblName +"/"

    // [2-Depth]
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnLock_2D)  // Create Lock ZNode
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D)  // Create Read ZNode
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D)  // Create Write ZNode

    // [3-Depth]
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnActive_3D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnStandBy_3D)

    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnActive_3D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnStandBy_3D)

    // [4-Depth]
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblName_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblPath_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblName_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblPath_4D)

    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblName_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblPath_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblName_4D)
    ZKDistLock.createZNode(zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblPath_4D)
  }

  protected[util] def initializeZNodeData(dbName:String,tblName:String): Unit = {

    val rcConf = ConfigFactory.load(dbName + "/" + tblName)

    val read_active_tbl_name  = rcConf.getString(tblName+".read.active.tbl_name")
    val read_active_tbl_path  = rcConf.getString(tblName+".read.active.tbl_path")
    val read_standby_tbl_name  = rcConf.getString(tblName+".read.standby.tbl_name")
    val read_standby_tbl_path  = rcConf.getString(tblName+".read.standby.tbl_path")

    val write_active_tbl_name  = rcConf.getString(tblName+".write.active.tbl_name")
    val write_active_tbl_path  = rcConf.getString(tblName+".write.active.tbl_path")
    val write_standby_tbl_name  = rcConf.getString(tblName+".write.standby.tbl_name")
    val write_standby_tbl_path  = rcConf.getString(tblName+".write.standby.tbl_path")

    val zkTablePathPrefix = "/" + ZKDistLock.getZnAppRoot_0D + "/" + dbName + "/" + tblName +"/"

    // Set data for Active Read Tables
    val path_1 = zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblName_4D
    if(ZKDistLock.isExists(path_1))
      ZKDistLock.setData(path_1,read_active_tbl_name)

    val path_2 = zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblPath_4D
    if(ZKDistLock.isExists(path_2))
      ZKDistLock.setData(path_2,read_active_tbl_path)

    // Set data for StandBy Read Tables
    val path_3 = zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblName_4D
    if(ZKDistLock.isExists(path_3))
      ZKDistLock.setData(path_3,read_standby_tbl_name)

    val path_4 = zkTablePathPrefix + ZKDistLock.getZnRead_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblPath_4D
    if(ZKDistLock.isExists(path_4))
      ZKDistLock.setData(path_4,read_standby_tbl_path)

    // Set data for Active Write Tables
    val path_5 = zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblName_4D
    if(ZKDistLock.isExists(path_5))
      ZKDistLock.setData(path_5,write_active_tbl_name)

    val path_6 = zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnActive_3D + "/" + ZKDistLock.getZnTblPath_4D
    if(ZKDistLock.isExists(path_6))
      ZKDistLock.setData(path_6,write_active_tbl_path)

    // Set data for StandBy Write Tables
    val path_7 = zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblName_4D
    if(ZKDistLock.isExists(path_7))
      ZKDistLock.setData(path_7,write_standby_tbl_name)

    val path_8 = zkTablePathPrefix + ZKDistLock.getZnWrite_2D + "/" + ZKDistLock.getZnStandBy_3D + "/" + ZKDistLock.getZnTblPath_4D
    if(ZKDistLock.isExists(path_8))
      ZKDistLock.setData(path_8,write_standby_tbl_path)
  }

}
