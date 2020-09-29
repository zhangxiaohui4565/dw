drop table if exists gmall.dwd_display_log;
CREATE EXTERNAL TABLE gmall.dwd_display_log
(
    `mid_id`       string,
    `user_id`      string,
    `version_code` string,
    `version_name` string,
    `lang`         string,
    `source`       string,
    `os`           string,
    `area`         string,
    `model`        string,
    `brand`        string,
    `sdk_version`  string,
    `gmail`        string,
    `height_width` string,
    `app_time`     string,
    `network`      string,
    `lng`          string,
    `lat`          string,
    `action`       string,
    `goodsid`      string,
    `place`        string,
    `extend1`      string,
    `category`     string,
    `server_time`  string
)
    PARTITIONED BY (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_display_log/';