package com.jaeyeong.datalake.datapipeline.compaction

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BatchCompactionAppTest extends FlatSpec with BeforeAndAfter with Matchers {

  "assignFreshWriteActiveTbl" should "modify v_hbaccess view information" ignore  {

    //BatchCompactionApp.assignFreshWriteActiveTbl("hbaccess")
    true
  }


  "recreateHiveTbl" should "return COL_NAME:DATA_TYPE string sequence" in {

    BatchCompactionApp.recreateHiveTbl("inhouse_common", "hbaccess_write_a")
    true
  }

}
