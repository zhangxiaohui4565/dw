drop table if exists gmall.dwd_base_event_log;
CREATE EXTERNAL TABLE gmall.dwd_base_event_log
(
    `mid_id`       string comment '设备唯一标识-mid',
    `user_id`      string comment '用户标识-uid',
    `version_code` string comment '程序版本号-vc',
    `version_name` string comment '程序版本名称-vn',
    `lang`         string comment '系统语言-l',
    `source`       string comment '渠道号，应用从哪个渠道来的-s',
    `os`           string comment '系统',
    `area`         string comment '区域-ar',
    `model`        string comment '手机型号-md',
    `brand`        string comment '手机型号-bd',
    `sdk_version`  string comment 'sv',
    `gmail`        string,
    `height_width` string comment '高度宽度-hw',
    `app_time`     string comment '客户端产生的时间-t',
    `network`      string comment '网络模式-nw',
    `lng`          string comment '精度-ln',
    `lat`          string comment '维度-la',
    `event_name`   string,
    `event_json`   string,
    `server_time`  string comment '手机时间-ts'
)
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_base_event_log/'
    TBLPROPERTIES ('parquet.compression' = 'snappy');