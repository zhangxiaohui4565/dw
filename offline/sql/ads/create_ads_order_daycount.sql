drop table if exists gmall.ads_order_daycount;
create external table gmall.ads_order_daycount
(
    dt           string comment '统计日期',
    order_count  bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users  bigint comment '单日下单用户数'
) comment '下单数目统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_order_daycount';