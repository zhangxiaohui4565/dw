drop table if exists gmall.dwd_fact_coupon_use;
create external table gmall.dwd_fact_coupon_use
(
    `id`            string COMMENT '编号',
    `coupon_id`     string COMMENT '优惠券ID',
    `user_id`       string COMMENT 'userid',
    `order_id`      string COMMENT '订单id',
    `coupon_status` string COMMENT '优惠券状态',
    `get_time`      string COMMENT '领取时间',
    `using_time`    string COMMENT '使用时间(下单)',
    `used_time`     string COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_coupon_use/';