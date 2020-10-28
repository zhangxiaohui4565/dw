drop table if exists gmall.ads_product_favor_topN;
create external table gmall.ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';