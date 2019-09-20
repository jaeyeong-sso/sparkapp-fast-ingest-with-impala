package com.jaeyeong.datalake.datapipeline.schema

import org.apache.spark.sql.types.StructType

abstract class ISchemaLoader extends Serializable {

  val COLUMN_LIST_KEY = "columns"
  val PART_LIST_KEY = "partitions"
  val TBL_LOC_LIST_KEY = "locations"

  val MAP_KEY_NAME = "name"
  val MAP_KEY_TYPE = "type"
  val HIVE_RN_LOCATION_COL = "Location:"

  val dbName:String
  val tblName:String

  private[schema] def readSchemaResource : Map[String,Seq[Map[String,String]]]

  def getColSchemaWithPartStructType : StructType

  val tblSchemaObj = readSchemaResource

  val listColSchema = tblSchemaObj(COLUMN_LIST_KEY)
  val listPartSchema = tblSchemaObj(PART_LIST_KEY)
  val listLocSchema = tblSchemaObj(TBL_LOC_LIST_KEY)

}
