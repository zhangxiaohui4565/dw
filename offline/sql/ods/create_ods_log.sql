drop table if exists gmall.ods_log;
CREATE EXTERNAL TABLE ods_log
(
    `line` string
)
    PARTITIONED BY (`dt` string) -- 按照时间创建分区
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
    LOCATION '/warehouse/gmall/ods/ods_log'
;
-- 修复分区数据
msck repair table gmall.ods_log;