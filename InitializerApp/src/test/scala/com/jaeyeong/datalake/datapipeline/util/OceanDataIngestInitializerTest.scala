package com.jaeyeong.datalake.datapipeline.util

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class OceanDataIngestInitializerTest extends FlatSpec with Matchers {

  "Resource Config" should "load initialization data properly" ignore {
    val rcConf = ConfigFactory.load("hbaccess")
    val value  = rcConf.getString("hbaccess.read.active.tbl_name")
    value should not be null
  }

  "initializeZNodeData" should "load 'hbaccess.conf'" ignore {
    OceanDataIngestInitializer.initializePerTable("inhouse_common","hbaccess")
    true
  }

  "initializeZNodeData" should "load 'hbaccess_test.conf'" ignore {
    OceanDataIngestInitializer.initializePerTable("inhouse_common","hbaccess_test")
    true
  }
}
