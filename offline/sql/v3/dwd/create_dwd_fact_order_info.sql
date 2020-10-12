drop table if exists gmall.dwd_fact_order_info;
create external table gmall.dwd_fact_order_info
(
    `id`                    string COMMENT '订单编号',
    `order_status`          string COMMENT '订单状态',
    `user_id`               string COMMENT '用户id',
    `out_trade_no`          string COMMENT '支付流水号',
    `create_time`           string COMMENT '创建时间(未支付状态)',
    `payment_time`          string COMMENT '支付时间(已支付状态)',
    `cancel_time`           string COMMENT '取消时间(已取消状态)',
    `finish_time`           string COMMENT '完成时间(已完成状态)',
    `refund_time`           string COMMENT '退款时间(退款中状态)',
    `refund_finish_time`    string COMMENT '退款完成时间(退款完成状态)',
    `province_id`           string COMMENT '省份ID',
    `activity_id`           string COMMENT '活动ID',
    `original_total_amount` decimal(16, 2) COMMENT '原价金额',
    `benefit_reduce_amount` decimal(16, 2) COMMENT '优惠金额',
    `feight_fee`            decimal(16, 2) COMMENT '运费',
    `final_total_amount`    decimal(16, 2) COMMENT '订单金额'
) COMMENT '订单事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_info/';