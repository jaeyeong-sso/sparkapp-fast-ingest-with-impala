package com.jaeyeong.datalake.datapipeline.lock

import com.typesafe.config.ConfigFactory
import com.twitter.util.{Await, Future, JavaTimer}
import com.twitter.conversions.time._
import com.twitter.zk
import com.twitter.zk.{RetryPolicy, _}
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent}
import org.apache.zookeeper.ZooDefs.Ids

import scala.collection.JavaConverters._

class ZKDistLock (
                   protected[lock] val zkServer: Option[String],
                   protected[lock] val zkCli : ZkClient
                 ){

  final protected[lock] val SEQ_NODE_PREFIX = "SEQ"

  def syncGetChildren(path:String) = {
    val res = Await.result(zkCli(path).getChildren())
    res.children.sortWith( _.name < _.name )
  }

  def syncPeekChildren(path:String) = {
    syncGetChildren(path).head
  }

  def syncIsExists(path:String) = {
    try{
      Await.result(zkCli(path).exists()) match {
        case ZNode.Exists(str, stat) => true
      }
    } catch {
      case e : KeeperException.NoNodeException => false
    }
  }

  def syncGetData(path:String): Option[String] ={
    try{
      Await.result(zkCli(path).getData()) match {
        case ZNode.Data(str, stat, bytes) => {
          Some(new String(bytes))
        }
      }
    } catch {
      case e : KeeperException => None
    }
  }

  def syncSetData(path:String, data:String): Unit ={
    Await.result(zkCli(path).setData(data.getBytes(),-1))
  }

  /*
  def asyncSetWatchOnChildrenChanged(path:String) = {

    Await.result(zkCli(path).getChildren.watch().onSuccess {
      case ZNode.Watch(result, future: Future[WatchedEvent]) => {
        result.onSuccess {
          case ZNode.Children(path, stat, children) => {
              future.onSuccess {
                case NodeEvent.ChildrenChanged(path) => {
                  println("NodeEvent.ChildrenChanged : " + path)
                }
                case _ => None
              }
          }
        }
      }
    })
  }
  */

  def syncCreateZNode(path:String,data:String="",mode: CreateMode = CreateMode.PERSISTENT) : Option[ZNode] = {
    val res = Await.result(
      zkCli(path).create(data.getBytes, Ids.OPEN_ACL_UNSAFE.asScala, mode)
      .onSuccess{ znode => znode}
      .handle{ case e: KeeperException.NoNodeException => null }
    )
    res match {
      case znode:ZNode => Some(znode)
      case _ => None
    }
  }

  def syncDeleteZNode(path:String) : Option[ZNode] = {
    val res = Await.result(zkCli(path).delete(-1)
        .onSuccess{ znode => znode}
        .handle{ case e: KeeperException => null}
    )
    res match {
      case znode:ZNode => Some(znode)
      case _ => None
    }
  }

  /*
  def asyncAcquireLock(path:String) : Unit ={

    val newZNode = syncCreateZNode(path + "/" + SEQ_NODE_PREFIX, mode=CreateMode.EPHEMERAL_SEQUENTIAL)
    val curLockZNode = syncPeekChildren(path)

    if(!(newZNode.getOrElse("").equals(curLockZNode.path))){
      asyncSetWatchOnChildrenChanged(path)
    }
  }
  */

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // (1) Time interval 1 sec : to compare the most sequence number preceding ZNode
  // (2) Time interval 5 sec : in case Compaction App recreate parent lock node
  //                           to reset sequence number(to prevent sequence number overflow situation),
  //                           ZNode create action could be failed for a while.
  //                           At this time, retry to create node 5sec later.
  //                           (Compaction App will recreate parent lock node)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def syncAcquireLock(path:String) : ZNode ={

    def recvCheckActiveLockNode(targetZNode:ZNode) : ZNode = {
      val curLockZNode = syncPeekChildren(path)
      if(targetZNode.path.equals(curLockZNode.path)){
        targetZNode
      } else {
        Thread.sleep(1000)
        recvCheckActiveLockNode(targetZNode)
      }
    }

    val newZNode:ZNode = {
      val tryFirst = syncCreateZNode(path + "/" + SEQ_NODE_PREFIX, mode=CreateMode.EPHEMERAL_SEQUENTIAL).getOrElse(None)
      tryFirst match {
        case createZNode:ZNode => (
          createZNode
        )
        case None => {
          syncCreateZNode(path,mode = CreateMode.PERSISTENT)  // Create rw-lock node
          syncCreateZNode(path + "/" + SEQ_NODE_PREFIX, mode=CreateMode.EPHEMERAL_SEQUENTIAL).getOrElse(null)
        }
      }
    }

    if (newZNode!=null) recvCheckActiveLockNode(newZNode)
    else null
  }

  def syncReleaseLock(path:String) ={
    syncDeleteZNode(path)
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

object ZKDistLock {

  val rcConf = ConfigFactory.load("zookeeper")

  // START - Configuration properties
  val zkServer = {
    rcConf.hasPath("zkhost.SERVER") match {
      case true => Some(rcConf.getString("zkhost.SERVER"))
      case false => None
    }
  }

  protected[lock] val (
    znAppRoot_0D,
    znLock_2D,znRead_2D,znWrite_2D,
    znActive_3D,znStandBy_3D,
    znTblName_4D,znTblPath_4D
  ) = {

    val rootNode = rcConf.hasPath("znode_schema.ZN_APP_ROOT_0D")  match {
      case true => rcConf.getString("znode_schema.ZN_APP_ROOT_0D")
      case false => "oceanapp-default"
    }
    val lockNode = rcConf.hasPath("znode_schema.ZN_LOCK_2D")  match {
      case true => rcConf.getString("znode_schema.ZN_LOCK_2D")
      case false => "lock-default"
    }
    val readNode = rcConf.hasPath("znode_schema.ZN_READ_2D")  match {
      case true => rcConf.getString("znode_schema.ZN_READ_2D")
      case false => "read-default"
    }
    val writeNode = rcConf.hasPath("znode_schema.ZN_WRITE_2D")  match {
      case true => rcConf.getString("znode_schema.ZN_WRITE_2D")
      case false => "write-default"
    }
    val activeNode = rcConf.hasPath("znode_schema.ZN_ACTIVE_3D")  match {
      case true => rcConf.getString("znode_schema.ZN_ACTIVE_3D")
      case false => "active-default"
    }
    val standByNode = rcConf.hasPath("znode_schema.ZN_STANDBY_3D")  match {
      case true => rcConf.getString("znode_schema.ZN_STANDBY_3D")
      case false => "standby-default"
    }
    val tblNameNode = rcConf.hasPath("znode_schema.ZN_TBL_NAME_4D")  match {
      case true => rcConf.getString("znode_schema.ZN_TBL_NAME_4D")
      case false => "tbl-name-default"
    }
    val tblPathNode = rcConf.hasPath("znode_schema.ZN_TBL_PATH_4D")  match {
      case true => rcConf.getString("znode_schema.ZN_TBL_PATH_4D")
      case false => "tbl-path-default"
    }
    (rootNode,lockNode,readNode,writeNode,activeNode,standByNode,tblNameNode,tblPathNode)
  }

  // END - Configuration properties

  implicit val timer = new JavaTimer(true)
  val zkCli = ZkClient(zkServer.getOrElse("localhost:2181"), Some(5.seconds), 30.seconds).withRetries(3)    // ZKClient will retry up to 3times when connection error occur.

  private[lock] val zKDistLock = new ZKDistLock(zkServer,zkCli)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getZnAppRoot_0D = this.znAppRoot_0D
  def getZnLock_2D = this.znLock_2D
  def getZnRead_2D = this.znRead_2D
  def getZnWrite_2D = this.znWrite_2D
  def getZnActive_3D = this.znActive_3D
  def getZnStandBy_3D = this.znStandBy_3D
  def getZnTblName_4D = this.znTblName_4D
  def getZnTblPath_4D = this.znTblPath_4D

  def getZkServer = zKDistLock.zkServer

  def getChildren(path:String) = {
    zKDistLock.syncGetChildren(path)
  }

  def peekChildren(path:String) = {
    zKDistLock.syncGetChildren(path).head
  }

  def isExists(path:String) = {
    zKDistLock.syncIsExists(path)
  }

  def getData(path:String): Option[String] ={
    zKDistLock.syncGetData(path)
  }

  def setData(path:String, data:String): Unit ={
    zKDistLock.syncSetData(path,data)
  }

  /*
  def setWatchOnChildrenChanged(path:String) = {
    zKDistLock.asyncSetWatchOnChildrenChanged(path)
  }
  */

  def createZNode(path:String,data:String="",mode: CreateMode = CreateMode.PERSISTENT) : Option[ZNode] = {
    zKDistLock.syncCreateZNode(path,data,mode)
  }

  def deleteZNode(path:String) : Option[ZNode] = {
    zKDistLock.syncDeleteZNode(path)
  }

  def acquireLock(path:String) ={
    zKDistLock.syncAcquireLock(path)
  }

  def releaseLock(targetZNode:ZNode) ={
    zKDistLock.syncReleaseLock(targetZNode.path)
  }


  // Application utility functions.

  def acquireLogTypeLock(dbName:String, tblName:String) ={
    val logTypeLockPath = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + znLock_2D
    zKDistLock.syncAcquireLock(logTypeLockPath)
  }

  def getCurSpecificTblInfoWithMidPath(dbName:String, tblName:String, midPath:String) = {
    val zPathTblName = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + midPath + "/tbl-name"
    val zPathTblPath = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + midPath + "/tbl-path"

    ( zKDistLock.syncGetData(zPathTblName), zKDistLock.syncGetData(zPathTblPath) )
  }

  def switchActiveStandbyTbls(dbName:String, tblName:String,rwType:String): Boolean ={

    val pathActiveTblName = "/" + znAppRoot_0D + "/"  + dbName + "/" + tblName + "/" + rwType + "/active/tbl-name"
    val pathStandbyTblName = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + rwType + "/standby/tbl-name"
    val pathActiveTblPath = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + rwType + "/active/tbl-path"
    val pathStandbyTblPath = "/" + znAppRoot_0D + "/" + dbName + "/" + tblName + "/" + rwType + "/standby/tbl-path"

    val dataActiveTblName = zKDistLock.syncGetData(pathActiveTblName)
    val dataStandbyTblName = zKDistLock.syncGetData(pathStandbyTblName)
    val dataActiveTblPath = zKDistLock.syncGetData(pathActiveTblPath)
    val dataStandbyTblPath = zKDistLock.syncGetData(pathStandbyTblPath)

    if( (dataActiveTblName.getOrElse(null ) == null) ||
    (dataActiveTblName.getOrElse(null ) == null) ||
    (dataActiveTblName.getOrElse(null ) == null) ||
    (dataActiveTblName.getOrElse(null ) == null)){
      false
    } else {
      zKDistLock.syncSetData(pathActiveTblName,dataStandbyTblName.get)
      zKDistLock.syncSetData(pathStandbyTblName,dataActiveTblName.get)
      zKDistLock.syncSetData(pathActiveTblPath,dataStandbyTblPath.get)
      zKDistLock.syncSetData(pathStandbyTblPath,dataActiveTblPath.get)
      true
    }
  }
}
