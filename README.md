# Project : fast-ingest-with-impala

- Materialization on "Ingest and Query “Fast Data” with Impala(Without Kudu)".<br>
  [REF]. https://blog.cloudera.com/blog/2015/11/how-to-ingest-and-query-fast-data-with-impala-without-kudu/
  
- Implemented with IntelliJ Multi modules for common dependencies(Zookeeper application lock impl & Hive metastore synchronization impl).

<p>

## StreamingLogIngestApp

###### [Purpose] 
- Ingestion (Frequency: Once per Minute)
    - Write the stream data(read from Kafka stream) to HDFS active-read table location with the sequential store-id partition.

###### [Usage] 
- sudo -u spark spark2-submit --master yarn --deploy-mode cluster --class com.jaeyeong.datalake.datapipeline.ingest.StreamingLogIngestApp StreamingLogIngestApp-1.0-SNAPSHOT-shaded.jar <br/>
<b>--db-name</b> [REQUIRED]  ${TARGET_DB} <br/>
<b>--tbl-name</b> [REQUIRED]  ${TARGET_TBL} <br/>
<b>--topic-name</b> [REQUIRED] ${KAFKA_TOPIC_NAME} <br/>
<b>--coalesce-level</b> (default 1, merge to X files under the each partition) <br/>
<b>--batch-interval</b> 30 (second, default:300, minimum larger than 10) <br/>
<b>--use-start-offset-config</b> false(if true, read from old specific kafka offset per each partition) <br/>
<b>--debug-master-mode</b> local(only for IDEA debugging purpose) <br/>

<p>

## BatchCompactionApp

###### [Purpose] 
- Compaction (Frequency: Daily)
    - Switch active-write table pointer.
    - Copy data from active-read table to standby-read table.
    - Swap the read-table pointer and truncate standby-write table data.

###### [Usage] 
- sudo -u spark spark2-submit --master yarn --deploy-mode cluster --class com.jaeyeong.datalake.datapipeline.compaction.BatchCompactionApp BatchCompactionApp-1.0-SNAPSHOT.jar <br/>
<b>--db-name</b> [REQUIRED] ${TARGET_DB} <br/>
<b>--tbl-name</b> [REQUIRED] ${TARGET_TBL} <br/>
<b>--repartition-factor</b> (default 20, repartitioning & merge to X files under the each partition) <br/>

<p>

## MetaDataIngestInitializer

###### [Purpose] 
- Generate a ZNode data set per each Impala table : to support Read/Write Lock. 
<img width="717" alt="fast-ingest-zk-node-hierarchy" src="https://user-images.githubusercontent.com/10162969/65316586-3712b280-dbd5-11e9-9564-7abc99f8dd37.png">

###### [Usage] 
- Run on the IDEA with parameter modification. <br>
  [e.g] MetaDataIngestInitializer.initializePerTable("inhouse_common","hbaccess")

