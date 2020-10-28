drop table if exists gmall.ads_appraise_bad_topN;
create external table gmall.ads_appraise_bad_topN
(
    `dt`                 string COMMENT '统计日期',
    `sku_id`             string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16, 2) COMMENT '差评率'
) COMMENT '商品差评率'
    row format delimited fields terminated by '\t'
    location '/waads_appraise_bad_topNrehouse/gmall/ads/ads_appraise_bad_topN';