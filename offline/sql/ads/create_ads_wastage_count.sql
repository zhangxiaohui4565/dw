drop table if exists gmall.ads_wastage_count;
create external table gmall.ads_wastage_count
(
    `dt`            string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) COMMENT '流失用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_wastage_count';