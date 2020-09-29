-- 点赞表
drop table if exists gmall.dwd_praise_log;
CREATE EXTERNAL TABLE gmall.dwd_praise_log
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
    `id`           string,
    `userid`       string,
    `target_id`    string,
    `type`         string,
    `add_time`     string,
    `server_time`  string
)
    PARTITIONED BY (dt string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_praise_log/';