drop table if exists gmall.dwd_fact_payment_info;
create external table gmall.dwd_fact_payment_info
(
    `id`              string COMMENT 'id',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `payment_amount`  decimal(16, 2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type`    string COMMENT '支付类型',
    `payment_time`    string COMMENT '支付时间',
    `province_id`     string COMMENT '省份ID'
) COMMENT '支付事实表表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_payment_info/'