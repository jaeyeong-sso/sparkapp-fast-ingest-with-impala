package com.jaeyeong.datalake.datapipeline.compaction

import scopt.OptionParser

case class Config(
                   strDbName : String = null,
                   strTblName : String = null,
                   iRepartitionFactor : Int = 20
                 )

object OptionArgsParser {

  val ARG_NAME_DB_NAME = "db-name"
  val ARG_NAME_TBL_NAME = "tbl-name"
  val ARG_NAME_REPARTITION_FACTOR = "repartition-factor"

  private[compaction] def parseOptionArgs(args: Array[String]) = {
    val parser = new OptionParser[Config]("BatchCompactionApp") {
      opt[String](ARG_NAME_DB_NAME).required().action(
        (x, c) => c.copy(strDbName = x) ).text(
        "'db-name' is required argument(should be matched with 'Hive target DataBase")
      opt[String](ARG_NAME_TBL_NAME).required().action(
        (x, c) => c.copy(strTblName = x) ).text(
        "'tbl-name' is required argument(should be matched with 'Hive target Table under the --db-name param)")
      opt[Int](ARG_NAME_REPARTITION_FACTOR).action(
        (x, c) => c.copy(iRepartitionFactor = x))
        .validate(x =>
          if (x > 0) success
          else failure("'coalesce-level' should be larger than 0")
        )
    }

    parser.parse(args, Config()).map{
      config => (
        config.strDbName,
        config.strTblName,
        config.iRepartitionFactor
      )
    }.getOrElse((null,null,20))
  }

}
