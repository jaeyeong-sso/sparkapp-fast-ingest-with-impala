package com.jaeyeong.datalake.datapipeline.ingest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.scalatest._


class StreamingLogIngestAppTest extends FlatSpec with BeforeAndAfter with Matchers {

    private val sparkSession = SparkSession.builder()
      .appName("StreamingLogIngestAppTest")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._
    private val sctx = sparkSession.sparkContext


    "Time Conversion" should "be ok" ignore  {

        // 2017-08-06T00:00:00+09:00 , "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val df = sctx.parallelize(Seq("2017-08-06T00:00:00+09:00")).toDF("ts")

        val curTsCol = current_timestamp()

        val modifiedDf = df.withColumn("dt", date_format(col("ts"),"yyyyMMdd"))
                .withColumn("clusterTs", curTsCol)
                .withColumn("storeid", date_format(curTsCol,"hhmm"))

        modifiedDf.printSchema()
        modifiedDf.show()

        val dt_col_value = modifiedDf.select("dt").collect()
        dt_col_value(0) should be equals ("20170806")
    }
}