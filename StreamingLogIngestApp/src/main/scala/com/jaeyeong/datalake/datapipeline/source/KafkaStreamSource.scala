package com.jaeyeong.datalake.datapipeline.source

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils

import collection.JavaConversions._
import scala.collection.mutable.Map

class KafkaStreamSource(
                         sparkSessionParm:SparkSession,
                         ssctxParam:StreamingContext,
                         topicName:String,
                         bUseStartOffsetConfig:Boolean = false
                       ) {

  val sparkSession : SparkSession = sparkSessionParm
  val ssctx : StreamingContext = ssctxParam

  val rcConf = ConfigFactory.load("kafka_stream_source")
  val srcConfigParams = Map[String, Object]()

  this.srcConfigParams("bootstrap.servers") = rcConf.getString("source.kafka.bootstrap_servers")
  this.srcConfigParams("key.deserializer") = classOf[StringDeserializer]
  this.srcConfigParams("value.deserializer") = classOf[StringDeserializer]
  this.srcConfigParams("group.id") = rcConf.getString("source.kafka.group_id")
  this.srcConfigParams("auto.offset.reset") = rcConf.getString("source.kafka.auto_offset_reset")
  this.srcConfigParams("enable.auto.commit") = (false: java.lang.Boolean)

  val topics = Array(topicName)

  def getSourceInputDStream : InputDStream[ConsumerRecord[String, String]] = {

    val inputStream = getFromOffsetMapFromConfig match {
      case Some(fromOffsets) => {
        KafkaUtils.createDirectStream[String, String](
          ssctx,
          PreferConsistent,
          Subscribe[String, String](topics,this.srcConfigParams,fromOffsets)
        )

      }
      case None => {
        KafkaUtils.createDirectStream[String, String](
          ssctx,
          PreferConsistent,
          Subscribe[String, String](topics, this.srcConfigParams)
        )
      }
    }
    inputStream
  }

  private[source] def getFromOffsetMapFromConfig: Option[Map[TopicPartition,Long]] = {

    val configPath = "source.kafka.custom_from_offsets"
    val rcConf = ConfigFactory.load

    if(bUseStartOffsetConfig && rcConf.hasPath(configPath)){
      val fromOffsetConfList = rcConf.getConfigList(configPath)

      val fromOffsetMap : Map[TopicPartition,Long] = fromOffsetConfList.foldLeft(Map[TopicPartition, Long]()) {
        (offsetKeyMap:Map[TopicPartition, Long], confElem) => {

          val topic:String = confElem.getString("topic")
          val partitionId:Int = confElem.getInt("partition_id")
          val offset:Long = confElem.getLong("offset")

          (offsetKeyMap + ((new TopicPartition(topic, partitionId) -> offset)))
        }
      }
      Some(fromOffsetMap)
    } else {
      None
    }
  }

}
