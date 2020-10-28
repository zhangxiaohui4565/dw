drop table if exists gmall.ads_silent_count;
create external table gmall.ads_silent_count
(
    `dt`           string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_silent_count';