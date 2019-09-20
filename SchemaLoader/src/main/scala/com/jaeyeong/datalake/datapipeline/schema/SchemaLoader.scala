package com.jaeyeong.datalake.datapipeline.schema

import org.apache.spark.sql.types.StructType

import scala.io.Source
import scala.util.parsing.json.JSON

class SchemaLoader(val dbName:String, val tblName:String) extends ISchemaLoader {

  private[schema] override  def readSchemaResource : Map[String,Seq[Map[String,String]]] = {
    val srcStream = getClass.getResourceAsStream("/tbls_schema_template/" + dbName + "/" + tblName + ".json")
    val source: String = Source.fromInputStream(srcStream).getLines.mkString
    JSON.parseFull(source).getOrElse(Map()).asInstanceOf[Map[String,Seq[Map[String,String]]]]
  }

  override def getColSchemaWithPartStructType : StructType = {
    val seqCols = tblSchemaObj.getOrElse(COLUMN_LIST_KEY,Nil) ++ tblSchemaObj.getOrElse(PART_LIST_KEY,Nil)

    val schemaStructTypeBuilder = seqCols.foldLeft(new StructType()){
      (structTypeObj: StructType, colMap: Map[String, String]) => {
        val retStructTypeObj = structTypeObj.add(colMap.getOrElse(MAP_KEY_NAME, ""), colMap.getOrElse(MAP_KEY_TYPE, ""))
        retStructTypeObj
      }
    }
    schemaStructTypeBuilder
  }


  /*
  override def getColSchemaInfo = {
    val listColSchema = readSchemaResource.getOrElse("columns",Nil).asInstanceOf[List[Map[String, String]]]
    listColSchema.toSeq
    /*
    val colSchemaStructTypeBuilder = listColSchema.foldLeft(new StructType()) {
      (structTypeObj: StructType, colMap: Map[String, String]) => {
        val retStructTypeObj = structTypeObj.add(colMap.getOrElse("name", ""), colMap.getOrElse("type", ""))
        retStructTypeObj
      }
    }
    colSchemaStructTypeBuilder
    */
  }

  override def getPartSchemaInfo  = {
    val partColSchema = readSchemaResource.getOrElse("partitions",Nil).asInstanceOf[List[Map[String, String]]]
    partColSchema.toSeq

    /*
    val srcStream = getClass.getResourceAsStream("/tbls_schema_template/" + dbName + "/" + tblName + ".json")
    val source: String = Source.fromInputStream(srcStream).getLines.mkString
    val partitions = JSON.parseFull(source).getOrElse(Map()).asInstanceOf[Map[String,List[Map[String,String]]]].getOrElse("partitions",Nil)

    val res = partitions.foldLeft(ListBuffer[(String,String)]()){
      ( seqRes:ListBuffer[(String,String)], map:Map[String,String] ) => {
        seqRes += Tuple2(map.getOrElse("name","").toString, map.getOrElse("type","").toString)
      }
    }
    res.toSeq
    */
  }
  */

  /*
  def getAllSchemaStructType = {
    val listColSchema = getSchemaResource.asInstanceOf[List[Map[String,String]]]
    val colSchemaStructTypeBuilder = listColSchema.foldLeft(new StructType()) {
      (structTypeObj:StructType, colMap:Map[String,String]) => {
        val retStructTypeObj = structTypeObj.add(colMap.getOrElse("name",""),colMap.getOrElse("type",""))
        retStructTypeObj
      }
    }
    val seqPartSchema = getSchemaPartitionInfo.asInstanceOf[Seq[(String,String)]]
    val partSchemaStructTypeBuilder = seqPartSchema.foldLeft(colSchemaStructTypeBuilder){
      (structTypeObj:StructType, partsTuple:(String,String)) => {
        val retStructTypeObj = structTypeObj.add(partsTuple._1,partsTuple._2)
        retStructTypeObj
      }
    }
    partSchemaStructTypeBuilder
  }
  */
}
