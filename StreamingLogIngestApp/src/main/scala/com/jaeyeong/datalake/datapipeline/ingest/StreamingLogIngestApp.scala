package com.jaeyeong.datalake.datapipeline.ingest

import com.jaeyeong.datalake.datapipeline.jdbcconn.{HiveJDBCConn, ImpalaJDBCConn}
import com.jaeyeong.datalake.datapipeline.lock.ZKDistLock
import com.jaeyeong.datalake.datapipeline.schema.{HiveMetaStrategy, SchemaLoader}
import com.jaeyeong.datalake.datapipeline.source.KafkaStreamSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.sql.functions._

import scala.annotation._

object StreamingLogIngestApp extends App with Serializable {

    val (
      dbName:String,
      tblName:String,
      batchInterval:Long,
      bUseStartOffsetConfig:Boolean,
      iCoalesceLevel:Int,
      topicName:String,
      debugMasterMode:String
      ) = OptionArgsParser.parseOptionArgs(this.args)

    processSparkStreamingContext(dbName,tblName,batchInterval,bUseStartOffsetConfig,iCoalesceLevel,topicName,debugMasterMode)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private def processSparkStreamingContext(
                                              dbName:String,
                                              tblName:String,
                                              batchInterval:Long,
                                              bUseStartOffsetConfig:Boolean,
                                              iCoalesceLevel:Int,
                                              topicName:String,
                                              debugMasterMode:String ) = {

      // Initialize SparkSession diversely according to "local"(or "local[*]")  directive.
      val sparkSession =  if(debugMasterMode.contains("local")) {
        SparkSession
          .builder()
          .appName("StreamingLogIngestApp")
          .master(debugMasterMode)
          .config("spark.streaming.stopGracefullyOnShutdown","true")
          .getOrCreate()
      } else {
        SparkSession
          .builder()
          .appName("StreamingLogIngestApp")
          .config("spark.streaming.stopGracefullyOnShutdown","true")
          .getOrCreate()
      }

      val sqlctx = sparkSession.sqlContext
      val ssctx = new StreamingContext(sparkSession.sparkContext, Seconds(batchInterval))   // SparkStreaming micro-batch interval default value is 300sec(5min).

      // Spark DStream via KafkaSource
      val dataSStreamSource = new KafkaStreamSource(sparkSession,ssctx,topicName,bUseStartOffsetConfig)
      val dstream = dataSStreamSource.getSourceInputDStream

      // [To-Do] Should be modified !!
      def processOutDataframe(df:DataFrame) : DataFrame = {
        val curTsCol = current_timestamp()

        val modifiedDf = df
          //  [Fix-Me #1]:  according to "Fix-Me #2" changes, "upper" usage might be removed.
          .withColumn("dt", date_format(upper(col("ts")),"yyyyMMdd"))
          .withColumn("appid", col("appId"))
          .withColumn("storeid", date_format(curTsCol,"HHmm"))

        if(debugMasterMode.contains("local"))
          modifiedDf.show(10)

        modifiedDf
      }

      // Process RDD on the basis of partition
      //  (1). Apply parquet column schema to DF.
      //  (2). Acquire Zookeeper Write Lock(await way) -> Critical Section started.
      //  (3). Write DF data to HDFS with specific coalesce factor(guarantee to prevent too many small files).
      //  (4). Make Hive/Impala aware the meta(partitions/files) changes.
      //  (5). Acquire Zookeeper Write Lock(await way) -> Critical Section finished.
      def processPartition (rdd:RDD[ConsumerRecord[String,String]]) = {

          val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          val trLockNode = ZKDistLock.acquireLogTypeLock(dbName,tblName)
          val (tbl_name,tbl_path) = ZKDistLock.getCurSpecificTblInfoWithMidPath(dbName,tblName,"write/active")

          val bContinueToDoTransaction =
            if( !(tbl_name.getOrElse(null).equals(null)) && !(tbl_path.getOrElse(null).equals(null))) true
            else false

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////
          // [START] Write/Update Meta Transaction (critical section)
          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          if(bContinueToDoTransaction) {

            // Loading Parquet column schema by strategy (default is to load from application resource, i.e. 'hbaccess.json'(under 'tbls_schema_template/inhouse_common').)
            val schemaLoader = new SchemaLoader(dbName,tbl_name.get) with HiveMetaStrategy

            // Apply StructType Schema to DF.
            val structuredDf = sqlctx.read.schema(
                schemaLoader.getColSchemaWithPartStructType
              ).json(rdd.map(record => record.value().toLowerCase //  [Fix-Me #2]: if the key of kafka topic message is low-case string, ".toLowCase" would not be required.
            ))

            val outDf = processOutDataframe(structuredDf)

            // [Fix-Me #3]: "appid" could be differ according to partition definition.
            outDf.coalesce(iCoalesceLevel).write.
              partitionBy("dt","appid","storeid").
              format("parquet").
              mode(SaveMode.Append).
              save(tbl_path.get)

            // Renew Metadata
            HiveJDBCConn.execute("MSCK REPAIR TABLE " + dbName + "." + tbl_name.get)
            ImpalaJDBCConn.execute("REFRESH " + dbName + "." + tbl_name.get)
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////
          // [END] Write/Update Meta Transaction (critical section)
          //////////////////////////////////////////////////////////////////////////////////////////////////////////////
          ZKDistLock.releaseLock(trLockNode)

          if(bContinueToDoTransaction)
            dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
      }

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // Spark Streaming micro-batch iteration
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      dstream.foreachRDD { rdd =>
        if(!rdd.isEmpty())
          processPartition(rdd)
      }
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      ssctx.start()
      ssctx.awaitTermination()

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Utility Functions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @deprecated
    private def printConsumerRecordValues(rdd:RDD[ConsumerRecord[String,String]]) = {

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { consumerRecords =>
        val o: OffsetRange = offsets(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        consumerRecords.foreach(
          record => println(record.value())
        )
      }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}
