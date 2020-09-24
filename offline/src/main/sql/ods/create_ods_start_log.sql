drop table if exists gmall.ods_start_log;
CREATE EXTERNAL TABLE gmall.ods_start_log (`line` string)
PARTITIONED BY (`dt` string)
STORED AS orc  TBLPROPERTIES ("orc.compress"="SNAPPY");
LOCATION '/warehouse/gmall/ods/ods_start_log';