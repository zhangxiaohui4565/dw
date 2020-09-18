drop table if exists gmall.ods_event_log;
CREATE EXTERNAL TABLE gmall.ods_event_log(`line` string)
PARTITIONED BY (`dt` string)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_event_log';

load data inpath '/origin_data/gmall/log/topic_event/2020-09-15' into table gmall.ods_event_log partition(dt='2020-09-15');
load data inpath '/origin_data/gmall/log/topic_event/2020-09-16' into table gmall.ods_event_log partition(dt='2020-09-16');
load data inpath '/origin_data/gmall/log/topic_event/2020-09-17' into table gmall.ods_event_log partition(dt='2020-09-17');

show partitions ods_event_log