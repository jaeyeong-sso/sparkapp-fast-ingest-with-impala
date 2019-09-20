CREATE DATABASE inhouse_common LOCATION 'hdfs://inhouseraw/data/rawlog/common.db';

CREATE EXTERNAL TABLE inhouse_common.hbaccess_read_a
(appVer STRING, osType STRING, osVer STRING, sdkVersion STRING, terminalId STRING, deviceName STRING, country STRING, language STRING, mid STRING, networkType STRING, loginType STRING, carrier STRING, market STRING, ts STRING, clientIp STRING, userKey STRING, clientTs STRING, clientSdkVer STRING, sessionId STRING, uniqueLogId STRING, instanceId STRING, encTerminalId STRING, encMid STRING, encUserKey STRING, httpStatusCode STRING)
PARTITIONED BY (dt STRING, appid STRING) STORED AS PARQUET
LOCATION 'hdfs://inhouseraw/data/rawlog/common.db/hbaccess_read';

CREATE EXTERNAL TABLE inhouse_common.hbaccess_read_b
(appVer STRING, osType STRING, osVer STRING, sdkVersion STRING, terminalId STRING, deviceName STRING, country STRING, language STRING, mid STRING, networkType STRING, loginType STRING, carrier STRING, market STRING, ts STRING, clientIp STRING, userKey STRING, clientTs STRING, clientSdkVer STRING, sessionId STRING, uniqueLogId STRING, instanceId STRING, encTerminalId STRING, encMid STRING, encUserKey STRING, httpStatusCode STRING)
PARTITIONED BY (dt STRING, appid STRING) STORED AS PARQUET
LOCATION 'hdfs://inhouseraw/data/rawlog/common.db/hbaccess_read';

CREATE EXTERNAL TABLE inhouse_common.hbaccess_write_a
(appVer STRING, osType STRING, osVer STRING, sdkVersion STRING, terminalId STRING, deviceName STRING, country STRING, language STRING, mid STRING, networkType STRING, loginType STRING, carrier STRING, market STRING, ts STRING, clientIp STRING, userKey STRING, clientTs STRING, clientSdkVer STRING, sessionId STRING, uniqueLogId STRING, instanceId STRING, encTerminalId STRING, encMid STRING, encUserKey STRING, httpStatusCode STRING)
PARTITIONED BY (dt STRING, appid STRING, storeid STRING) STORED AS PARQUET
LOCATION 'hdfs://inhouseraw/data/rawlog/common.db/hbaccess_write_a';

CREATE EXTERNAL TABLE inhouse_common.hbaccess_write_b
(appVer STRING, osType STRING, osVer STRING, sdkVersion STRING, terminalId STRING, deviceName STRING, country STRING, language STRING, mid STRING, networkType STRING, loginType STRING, carrier STRING, market STRING, ts STRING, clientIp STRING, userKey STRING, clientTs STRING, clientSdkVer STRING, sessionId STRING, uniqueLogId STRING, instanceId STRING, encTerminalId STRING, encMid STRING, encUserKey STRING, httpStatusCode STRING)
PARTITIONED BY (dt STRING, appid STRING, storeid STRING) STORED AS PARQUET
LOCATION 'hdfs://inhouseraw/data/rawlog/common.db/hbaccess_write_b';

-- CREATE VIEW inhouse_common.v_hbaccess AS SELECT * FROM inhouse_common.hbaccess_read_a UNION ALL SELECT * FROM inhouse_common.hbaccess_write_a;
CREATE VIEW inhouse_common.v_hbaccess AS SELECT appVer,osType,osVer,sdkVersion,terminalId,deviceName,country,language,mid,networkType,loginType,carrier,market,ts,clientIp,userKey,clientTs,clientSdkVer,sessionId,uniqueLogId,instanceId,encTerminalId,encMid,encUserKey,httpStatusCode FROM inhouse_common.hbaccess_read_a UNION ALL SELECT appVer,osType,osVer,sdkVersion,terminalId,deviceName,country,language,mid,networkType,loginType,carrier,market,ts,clientIp,userKey,clientTs,clientSdkVer,sessionId,uniqueLogId,instanceId,encTerminalId,encMid,encUserKey,httpStatusCode FROM inhouse_common.hbaccess_write_a;
