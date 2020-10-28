drop table if exists gmall.ads_area_topic;
create external table gmall.ads_area_topic
(
    `dt`                 string COMMENT '统计日期',
    `id`                 bigint COMMENT '编号',
    `province_name`      string COMMENT '省份名称',
    `area_code`          string COMMENT '地区编码',
    `iso_code`           string COMMENT 'iso编码',
    `region_id`          string COMMENT '地区ID',
    `region_name`        string COMMENT '地区名称',
    `login_day_count`    bigint COMMENT '当天活跃设备数',
    `order_day_count`    bigint COMMENT '当天下单次数',
    `order_day_amount`   decimal(16, 2) COMMENT '当天下单金额',
    `payment_day_count`  bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16, 2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_area_topic/';