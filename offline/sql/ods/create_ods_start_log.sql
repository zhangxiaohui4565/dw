drop table if exists gmall.ods_start_log;
CREATE EXTERNAL TABLE gmall.ods_start_log
(
    `line` string
)
    PARTITIONED BY (`dt` string)
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '/warehouse/gmall/ods/ods_start_log';
