drop table if exists gmall.dwd_error_log;
CREATE EXTERNAL TABLE gmall.dwd_error_log
(
    `area_code`       string COMMENT '地区编码',
    `brand`           string COMMENT '手机品牌',
    `channel`         string COMMENT '渠道',
    `model`           string COMMENT '手机型号',
    `mid_id`          string COMMENT '设备id',
    `os`              string COMMENT '操作系统',
    `user_id`         string COMMENT '会员id',
    `version_code`    string COMMENT 'app版本号',
    `page_item`       string COMMENT '目标id ',
    `page_item_type`  string COMMENT '目标类型',
    `last_page_id`    string COMMENT '上页类型',
    `page_id`         string COMMENT '页面ID ',
    `source_type`     string COMMENT '来源类型',
    `entry`           string COMMENT ' icon手机图标  notice 通知 install 安装后启动',
    `loading_time`    string COMMENT '启动加载时间',
    `open_ad_id`      string COMMENT '广告页ID ',
    `open_ad_ms`      string COMMENT '广告总共播放时间',
    `open_ad_skip_ms` string COMMENT '用户跳过广告时点',
    `actions`         string COMMENT '动作',
    `displays`        string COMMENT '曝光',
    `ts`              string COMMENT '时间',
    `error_code`      string COMMENT '错误码',
    `msg`             string COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (dt string)
    stored as parquet
    LOCATION '/warehouse/gmall/dwd/dwd_error_log';