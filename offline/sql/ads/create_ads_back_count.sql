drop table if exists gmall.ads_back_count;
create external table gmall.ads_back_count
(
    `dt`            string COMMENT '统计日期',
    `wk_dt`         string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流用户数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_back_count';