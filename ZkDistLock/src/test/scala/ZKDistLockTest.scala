package com.jaeyeong.datalake.datapipeline.lock

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Nil
import org.apache.zookeeper.CreateMode
import java.util.Calendar
import java.text.SimpleDateFormat

class ZKDistLockTest extends FlatSpec with Matchers  {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Configuration resource file loading test
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  "(0). Reading an exist value from application.conf" should "return 'vghdpnndv~'" ignore  {
    println ("zkServer >> " + ZKDistLock.getZkServer.getOrElse(""))
    ZKDistLock.getZkServer.getOrElse("") should be ("ZK_HOST:2181")
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Utility Test function for Operation test
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def prepareTestZPath(targetPath:String): Unit ={
    if(!ZKDistLock.isExists(targetPath)){
      ZKDistLock.createZNode(targetPath)
    }
  }

  def cleanupTestZPath(targetPath:String) = {
    ZKDistLock.deleteZNode(targetPath)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Basic ZNode Operation test
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  "(1). Listing via ZKClient" should "hadoop-ha , hive_zookeeper_namespace_hive , rmstore ,yarn-leader-election , zookeeper)" ignore {
    val childZnodes = ZKDistLock.getChildren("/")
    childZnodes.foreach(
      znode => println(znode.name)
    )
    childZnodes.size should be (5)
  }

  "(2-1). Create/Delete/IsExist via ZKClient" should "create /ocean-log-ingest/test_new_node then check the presence, finally delete." ignore {
    val testNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/" + "is-exist-test-node"
    val retNegative = ZKDistLock.isExists(testNodeName)
    ZKDistLock.createZNode(testNodeName)
    val retPositive = ZKDistLock.isExists(testNodeName)

    val resDelete = ZKDistLock.deleteZNode(testNodeName)

    (retNegative != retPositive) should equal(true)
  }

  "(2-2). Create new node Failed scenario via ZKClient" should "in case, the parent path is not exists." ignore {
    val retFailed = ZKDistLock.createZNode("/non-exist-root-path/create-test-node")
    retFailed should equal (None)
  }

  "(3). Get/Set Data" should "be exactly same." ignore {

    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val currentMinuteAsString = minuteFormat.format(now)

    val testNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/get-set-test-node"
    prepareTestZPath(testNodeName)

    ZKDistLock.setData(testNodeName,currentMinuteAsString)
    val res = ZKDistLock.getData(testNodeName)
    println("Get Data result : " + res.getOrElse(""))

    cleanupTestZPath(testNodeName)

    res.getOrElse("") should equal (currentMinuteAsString)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Applicative ZNode Operation test
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "(1). GetChildren(Sequence & ephemeral node)" should "return as created order" ignore  {

    val testNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/lock-test-node"

    cleanupTestZPath(testNodeName)
    prepareTestZPath(testNodeName)

    def action = ZKDistLock.createZNode(testNodeName + "/SEQ", mode = CreateMode.EPHEMERAL_SEQUENTIAL)
    val createNodes = action :: (action :: ( action :: (action :: (action :: Nil))))
    val existNodes = ZKDistLock.getChildren(testNodeName)


    val bCompared = createNodes.zip(existNodes).forall(
      tuple => {
        val left = tuple._1.getOrElse("")
        val right = testNodeName + "/" + tuple._2.name
        println(left + " : " + right)
        left.equals(right)
      }
    )
    bCompared should be (true)
  }

  "(2). Create Sequence & Ephemeral Node" should "create sequence & ephemeral node" ignore  {

    val testNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/lock-test-node"

    cleanupTestZPath(testNodeName)
    prepareTestZPath(testNodeName)

    def action = ZKDistLock.createZNode(testNodeName + "/SEQ", mode = CreateMode.PERSISTENT_SEQUENTIAL)

    val createNodes = action :: (action :: (action :: Nil))
    val existNodes = ZKDistLock.getChildren(testNodeName)

    createNodes.size should equal(existNodes.size)
  }

  "(3). Create Node considering NoNodeException" should "return NotNull(if exists) & Null(if not exists)." ignore  {

    val testNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/lock-test-node"

    this.prepareTestZPath(testNodeName)
    val resSuccess = ZKDistLock.createZNode(testNodeName + "/SEQ", mode = CreateMode.PERSISTENT_SEQUENTIAL)

    this.cleanupTestZPath(testNodeName)
    val resFail = ZKDistLock.createZNode(testNodeName + "/SEQ", mode = CreateMode.PERSISTENT_SEQUENTIAL)

    println("resSuccess : " + resSuccess.getOrElse(null).path)

    ((!resSuccess.getOrElse(null).equals(null)) && (resFail.getOrElse(null).equals(null))) should be (false)
  }

  "(4). Await to acquire Lock(/oceanapp-log-ingest/inhouse_common/hbaccess_test/rw-lock)" should "failed in case failed to create child sequence node twice" ignore  {
      val res = ZKDistLock.acquireLogTypeLock("inhouse_common","hbaccess_test")
      res should not be null
  }

  "(5). Get Data Wrapper(getCurSpecificTblInfoWithMidPath)" should "read all data precisely." ignore  {

    val resDataReadActive = ZKDistLock.getCurSpecificTblInfoWithMidPath("inhouse_common","hbaccess_test","read/active")
    val resDataReadStandby = ZKDistLock.getCurSpecificTblInfoWithMidPath("inhouse_common","hbaccess_test","read/standby")
    val resDataWriteActive = ZKDistLock.getCurSpecificTblInfoWithMidPath("inhouse_common","hbaccess_test","write/active")
    val resDataWriteStandby = ZKDistLock.getCurSpecificTblInfoWithMidPath("inhouse_common","hbaccess_test","write/standby")

    println(resDataReadActive._1 + " , " + resDataReadActive._2)
    println(resDataReadStandby._1 + " , " + resDataReadStandby._2)
    println(resDataWriteActive._1 + " , " + resDataWriteActive._2)
    println(resDataWriteStandby._1 + " , " + resDataWriteStandby._2)

    true
  }

  "(6). Run switchActiveStandbyTbls twice" should "make changes on the ZNode data to original state." ignore {

    val curReadActiveTblNameData = ZKDistLock.getData("/" + ZKDistLock.getZnAppRoot_0D + "inhouse_common/hbaccess_test/read/active/tbl-name")
    println("Current Active Read tbl name : " + curReadActiveTblNameData.getOrElse(""))

    ZKDistLock.switchActiveStandbyTbls("inhouse_common","hbaccess_test","read")
    val chagnedReadActiveTblNameData = ZKDistLock.getData("/" + ZKDistLock.getZnAppRoot_0D + "inhouse_common/hbaccess_test/read/active/tbl-name")
    println("Changed Active Read tbl name : " + chagnedReadActiveTblNameData.getOrElse(""))

    ZKDistLock.switchActiveStandbyTbls("inhouse_common","hbaccess_test","read")
    val rollbackReadActiveTblNameData = ZKDistLock.getData("/" + ZKDistLock.getZnAppRoot_0D + "inhouse_common/hbaccess_test/read/active/tbl-name")
    println("Rolled-back Active Read tbl name : " + rollbackReadActiveTblNameData.getOrElse(""))

    curReadActiveTblNameData.getOrElse("unexpected value #1") should be equals (rollbackReadActiveTblNameData.getOrElse("unexpected value #2"))
  }
}
