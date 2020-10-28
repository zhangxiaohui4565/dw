drop table if exists gmall.ads_payment_daycount;
create external table gmall.ads_payment_daycount
(
    dt                 string comment '统计日期',
    order_count        bigint comment '单日支付笔数',
    order_amount       bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count  bigint comment '单日支付商品数',
    payment_avg_time   decimal(16, 2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_payment_daycount';