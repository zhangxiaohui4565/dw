drop table if exists gmall.dwd_display_log;
CREATE EXTERNAL TABLE gmall.dwd_display_log
(
    `area_code`      string COMMENT '地区编码',
    `brand`          string COMMENT '手机品牌',
    `channel`        string COMMENT '渠道',
    `model`          string COMMENT '手机型号',
    `mid_id`         string COMMENT '设备id',
    `os`             string COMMENT '操作系统',
    `user_id`        string COMMENT '会员id',
    `version_code`   string COMMENT 'app版本号',
    `during_time`    bigint COMMENT 'app版本号',
    `page_item`      string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id`   string COMMENT '上页类型',
    `page_id`        string COMMENT '页面ID ',
    `source_type`    string COMMENT '来源类型',
    `ts`             bigint COMMENT 'app版本号',
    `display_type`   string COMMENT '曝光类型',
    `item`           string COMMENT '曝光对象id ',
    `item_type`      string COMMENT 'app版本号',
    `order`          bigint COMMENT '出现顺序'
) COMMENT '曝光日志表'
    PARTITIONED BY (dt string)
    stored as parquet
    LOCATION '/warehouse/gmall/dwd/dwd_display_log';