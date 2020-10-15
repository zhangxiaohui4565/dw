drop table if exists gmall.dws_sku_action_daycount;
create external table gmall.dws_sku_action_daycount
(
    sku_id                 string comment 'sku_id', -- dwd_fact_order_detail
    order_count            bigint comment '被下单次数', -- dwd_fact_order_detail
    order_num              bigint comment '被下单件数', -- dwd_fact_order_detail
    order_amount           decimal(16, 2) comment '被下单金额',  -- dwd_fact_order_detail
    payment_count          bigint comment '被支付次数', -- dwd_fact_order_detail
    payment_num            bigint comment '被支付件数', -- dwd_fact_order_detail
    payment_amount         decimal(16, 2) comment '被支付金额', -- dwd_fact_order_detail
    refund_count           bigint comment '被退款次数',  -- dwd_fact_order_refund_info
    refund_num             bigint comment '被退款件数', -- dwd_fact_order_refund_info
    refund_amount          decimal(16, 2) comment '被退款金额', -- dwd_fact_order_refund_info
    cart_count             bigint comment '被加入购物车次数', -- dwd_action_log cart_add
    favor_count            bigint comment '被收藏次数', -- dwd_action_log favor_add
    appraise_good_count    bigint comment '好评数', -- dwd_fact_comment_info
    appraise_mid_count     bigint comment '中评数', -- dwd_fact_comment_info
    appraise_bad_count     bigint comment '差评数', --dwd_fact_comment_info
    appraise_default_count bigint comment '默认评价数' -- dwd_fact_comment_info
) COMMENT '每日商品行为'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_sku_action_daycount/';