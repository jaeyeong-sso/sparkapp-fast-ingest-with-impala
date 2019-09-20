package com.jaeyeong.datalake.datapipeline.schema

import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.runtime.Nothing$

class SchemaReaderTest extends FlatSpec with Matchers {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  "Read schema from hbaccess.json" should "must have 27 valid fields" in {

    val schemaLoader = new SchemaLoader("inhouse_common","hbaccess")
    val retStructType:StructType = schemaLoader.getColSchemaWithPartStructType

    println(schemaLoader.listColSchema.mkString(",").toString)
    println(schemaLoader.listPartSchema.mkString(",").toString)
    println(schemaLoader.listLocSchema.mkString(",").toString)

    (retStructType.fields.length == 27 )  should be (true)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  "Read schema with HiveMetaStrategy" should "must have 28 valid fields" in  {

    val schemaLoader = new SchemaLoader("inhouse_common","hbaccess_write_a") with HiveMetaStrategy
    val retStructType:StructType = schemaLoader.getColSchemaWithPartStructType

    println(schemaLoader.listColSchema.mkString(",").toString)
    println(schemaLoader.listPartSchema.mkString(",").toString)
    println(schemaLoader.listLocSchema.mkString(",").toString)

    (retStructType.fields.length == 28 )  should be (true)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}
