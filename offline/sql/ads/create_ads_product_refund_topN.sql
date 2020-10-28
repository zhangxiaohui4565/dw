drop table if exists gmall.ads_product_refund_topN;
create external table gmall.ads_product_refund_topN
(
    `dt`           string COMMENT '统计日期',
    `sku_id`       string COMMENT '商品ID',
    `refund_ratio` decimal(16, 2) COMMENT '退款率'
) COMMENT '商品退款率排名'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_refund_topN';