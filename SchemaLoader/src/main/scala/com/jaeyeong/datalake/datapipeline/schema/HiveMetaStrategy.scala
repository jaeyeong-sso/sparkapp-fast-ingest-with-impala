package com.jaeyeong.datalake.datapipeline.schema

import com.jaeyeong.datalake.datapipeline.jdbcconn.HiveJDBCConn

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait HiveMetaStrategy extends ISchemaLoader {

  class TblStruct {
    var cols : mutable.Seq[(String,String)] = mutable.Seq[(String,String)]()
    var parts : mutable.Seq[(String,String)] = mutable.Seq[(String,String)]()
    var details : mutable.Seq[(String,String)] = mutable.Seq[(String,String)]()
    var storage : mutable.Seq[(String,String)] = mutable.Seq[(String,String)]()
    var idxMetaAreaToBeParse = 0
  }

  private def getDescirbeTblInfo() : TblStruct = {

    val delimiters = ("# col_name","# Partition","# Detailed","# Storage")
    val HIVE_FN_COL_NAME = "col_name"       // Hive Field Name : "col_name" of DESCRIBE FORMATTED result.
    val HIVE_FN_DATA_TYPE = "data_type"     // Hive Field Name : "data_type" of DESCRIBE FORMATTED result.

    val res = HiveJDBCConn.executeQuery("DESCRIBE FORMATTED " + dbName + "." + tblName)

    val resDescObj = res.getOrElse(Nil).foldLeft(new TblStruct()){
      (tblStruct:TblStruct, map: mutable.Map[String,Object]) => {
        val colNameVal = map.getOrElse(HIVE_FN_COL_NAME,"")
        val dataTypeVal = map.getOrElse(HIVE_FN_DATA_TYPE,"")


        if(colNameVal!=null){
          val colName = colNameVal.toString
          val dataType = if(dataTypeVal!=null) dataTypeVal.toString else ""

          if(colName.startsWith(delimiters._1)){

          } else if(colName.startsWith(delimiters._2)){
            tblStruct.idxMetaAreaToBeParse = 1
          } else if(colName.startsWith(delimiters._3)){
            tblStruct.idxMetaAreaToBeParse = 2
          } else if(colName.startsWith(delimiters._4)){
            tblStruct.idxMetaAreaToBeParse = 3
          } else {
            if( !colName.trim.isEmpty && !dataType.trim.isEmpty ){
              val newItem = (colName.trim,dataType.trim)
              tblStruct.idxMetaAreaToBeParse match {
                case 0 => tblStruct.cols = tblStruct.cols :+ newItem
                case 1 => tblStruct.parts = tblStruct.parts :+ newItem
                case 2 => tblStruct.details = tblStruct.details :+ newItem
                case 3 => tblStruct.storage = tblStruct.storage :+ newItem
              }
            }
          }
        }
        tblStruct
      }
    }
    resDescObj
  }

  private[schema] override def readSchemaResource : Map[String,List[Map[String,String]]] = {

    val tblStruct = getDescirbeTblInfo

    val resColListBuf = tblStruct.cols.foldLeft(ListBuffer[Map[String,String]]()){
      (resCols:ListBuffer[Map[String,String]], col:(String,String) ) => {
        resCols += Map(MAP_KEY_NAME -> col._1, MAP_KEY_TYPE -> col._2)
      }
    }
    val resColListMap = Map(COLUMN_LIST_KEY ->  resColListBuf.toList)

    val resPartListBuf = tblStruct.parts.foldLeft(ListBuffer[Map[String,String]]()){
      (resCols:ListBuffer[Map[String,String]], col:(String,String) ) => {
        resCols += Map(MAP_KEY_NAME -> col._1, MAP_KEY_TYPE -> col._2)
      }
    }
    val resPartListMap = resColListMap + (PART_LIST_KEY -> resPartListBuf.toList)

    val resDetailsBuf = tblStruct.details.filter(_._1.equals(HIVE_RN_LOCATION_COL)).foldLeft(ListBuffer[Map[String,String]]()){
      (resCols:ListBuffer[Map[String,String]], col:(String,String) ) => {
        resCols += Map(MAP_KEY_NAME -> col._1, MAP_KEY_TYPE -> col._2)
      }
    }
    val resTblLocListMap = resPartListMap + (TBL_LOC_LIST_KEY -> resDetailsBuf.toList)

    resTblLocListMap
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}



/*
override def getPartSchemaInfo : Seq[Map[String,String]] = {
  val tblStruct = getDescirbeTblInfo

  val resListBuf = tblStruct.parts.foldLeft(ListBuffer[(String,String)]()){
    ( seqRes:ListBuffer[(String,String)], tuple:Tuple2[String,String] ) => {
      seqRes += Map(tuple._1, tuple._2)
    }
  }
  resListBuf
}
*/

/*
def parseComplexTypeStr(str:String) = {

  //val FullName = """(\w+)[<](\w+)[:](\w+),(\w+),(\w+)[>]""".r

  //FullName findFirstIn "struct<appId:string,mid,ts>"

  //println(FullName)


  //val FullName(value1, value2,value3,value4,value5) = "struct<appId:string,mid,ts>"
  //println(value1, value2,value3,value4,value5)
}
*/

/*
val queryResult : Option[Seq[mutable.Map[String,Object]]] = HiveJDBCConn.executeQuery("DESCRIBE common." + this.logType)
queryResult.getOrElse(Seq[mutable.Map[String,Object]]()).foreach(
  rowMap => println(rowMap.getOrElse("col_name","") + " : " + rowMap.getOrElse("data_type",""))
)
*/
