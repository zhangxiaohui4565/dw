drop table if exists gmall.dwd_start_log;
CREATE EXTERNAL TABLE gmall.dwd_start_log
(
    `area_code`       string COMMENT '地区编码',
    `brand`           string COMMENT '手机品牌',
    `channel`         string COMMENT '渠道',
    `model`           string COMMENT '手机型号',
    `mid_id`          string COMMENT '设备id',
    `os`              string COMMENT '操作系统',
    `user_id`         string COMMENT '会员id',
    `version_code`    string COMMENT 'app版本号',
    `entry`           string COMMENT ' icon手机图标  notice 通知   install 安装后启动',
    `loading_time`    bigint COMMENT '启动加载时间',
    `open_ad_id`      string COMMENT '广告页ID ',
    `open_ad_ms`      bigint COMMENT '广告总共播放时间',
    `open_ad_skip_ms` bigint COMMENT '用户跳过广告时点',
    `ts`              bigint COMMENT '时间'
) COMMENT '启动日志表'
    PARTITIONED BY (dt string) -- 按照时间创建分区
    stored as parquet -- 采用parquet列式存储
    LOCATION '/warehouse/gmall/dwd/dwd_start_log'
;