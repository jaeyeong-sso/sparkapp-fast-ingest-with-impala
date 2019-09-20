package com.jaeyeong.datalake.datapipeline.source

import collection.JavaConversions._
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps
import scala.concurrent.duration._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.concurrent.Eventually
import org.apache.spark.streaming.ClockWrapper
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.TopicPartition

class KafkaSourceStrategyTest extends FlatSpec with Matchers with Eventually with BeforeAndAfter {

  private var sparkSession : SparkSession = _
  private var ssctx: StreamingContext = _
  private var clock: ClockWrapper = _

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  before {
    sparkSession = SparkSession.builder()
      .appName("KafkaSStreamSourceTest")
      .master("local")
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .getOrCreate()
    ssctx = new StreamingContext(sparkSession.sparkContext, Seconds(10))
    clock = new ClockWrapper(ssctx)
  }

  after{
    if (ssctx != null)
      ssctx.stop()
    if(sparkSession != null)
      sparkSession.stop()
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "KafkaSourceStrategy.getFromOffsetMapFromConfig() logic test" should "iterator config section and return offset Map[TopicPartition,Long] object" ignore {

    val rcConf = ConfigFactory.load
    val fromOffsetConfList = rcConf.getConfigList("source.kafka.custom-from-offsets")

    val fromOffsetMap : Map[TopicPartition,Long] = fromOffsetConfList.foldLeft(Map[TopicPartition, Long]()) {
      (offsetKeyMap:Map[TopicPartition, Long], confElem) => {
        val topic:String = confElem.getString("topic")
        val partitionId:Int = confElem.getInt("partition-id")
        val offset:Long = confElem.getLong("offset")
        (offsetKeyMap + ((new TopicPartition(topic, partitionId) -> offset)))
      }
    }
    fromOffsetMap.size should be (3)
  }

  "KafkaStreamSource" should "getSourceInputDStream return InputDStream[ConsumerRecord[String, String]]." ignore {

    val topicName = "RELEASE_gwc.v2.common.heartbeat"
    val dataSStreamSource = new KafkaStreamSource(sparkSession,ssctx,topicName)
    val dstream = dataSStreamSource.getSourceInputDStream

    dstream.foreachRDD { rdd =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { consumerRecords  =>
        //val o: OffsetRange = offsets(TaskContext.get.partitionId)
        //println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        consumerRecords.foreach( record =>
          println(record.value())
        )
      }

      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
    }

    ssctx.start()
    clock.advance(10000)
    eventually(timeout(1 seconds)){
      dstream should not equal null
    }
  }
}
