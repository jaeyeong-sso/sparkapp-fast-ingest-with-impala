package com.jaeyeong.datalake.datapipeline.ingest

import scopt.OptionParser

case class Config(
                   strDbName : String = null,
                   strTblName : String = null,
                   lBatchInterval : Long = 300,
                   bUseStartOffsetConfig: Boolean = false,
                   iCoalesceLevel : Int = 1,
                   strTopicName : String = null,
                   strDebugMasterMode : String = "yarn-cluster"
                 )

object OptionArgsParser {

  val ARG_NAME_DB_NAME = "db-name"
  val ARG_NAME_TBL_NAME = "tbl-name"
  val ARG_NAME_BATCH_INTERVAL = "batch-interval"
  val ARG_NAME_USE_START_OFFSET_CONF = "use-start-offset-config"
  val ARG_NAME_COALESCE_LEVEL = "coalesce-level"
  val ARG_NAME_TOPIC_NAME = "topic-name"
  val ARG_NAME_MASTER_MODE = "debug-master-mode"

  private[ingest] def parseOptionArgs(args:Array[String]) = {

    val parser = new OptionParser[Config]("StreamingLogIngestApp") {
      opt[String](ARG_NAME_DB_NAME).required().action(
        (x, c) => c.copy(strDbName = x) ).text(
        "'db-name' is required argument(should be matched with 'Hive target DataBase")
      opt[String](ARG_NAME_TBL_NAME).required().action(
        (x, c) => c.copy(strTblName = x) ).text(
        "'tbl-name' is required argument(should be matched with 'Hive target Table under the --db-name param)")
      opt[Int](ARG_NAME_BATCH_INTERVAL).action(
        (x, c) => c.copy(lBatchInterval = x) )
        .validate(x=>
          if(x > 10) success
          else failure("'batch-interval' should be bigger than 10(sec)")
        )
        .text("'batch-interval' is a numeric value(default is 300(5min)")
      opt[Unit](ARG_NAME_USE_START_OFFSET_CONF).action(
        (_, c) => c.copy(bUseStartOffsetConfig = true) ).text("'use-start-offset-config' is a flag")
      opt[Int](ARG_NAME_COALESCE_LEVEL).action(
        (x, c) => c.copy(iCoalesceLevel = x) )
        .validate(x=>
          if(x > 0) success
          else failure("'coalesce-level' should be larger than 0")
        )
        .text("'coalesce-level' is a numeric value(default is 1")
      opt[String](ARG_NAME_TOPIC_NAME).required().action(
        (x, c) => c.copy(strTopicName = x) ).text(
        "'topic-name' is required argument(i.g. Kafka Topic to be listened)")
      opt[String](ARG_NAME_MASTER_MODE).action(
        (x, c) => c.copy(strDebugMasterMode = x) ).text(
        "'debug-master-mode' is optional argument to debug development env(default:'yarn', selectively : 'local')")
    }

    parser.parse(args, Config()).map{
      config => (
        config.strDbName,
        config.strTblName,
        config.lBatchInterval,
        config.bUseStartOffsetConfig,
        config.iCoalesceLevel,
        config.strTopicName,
        config.strDebugMasterMode
      )
    }.getOrElse((null,null,10L,false,10,null,null))
  }
}
