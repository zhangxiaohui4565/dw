drop table if exists gmall.dws_activity_info_daycount;
create external table gmall.dws_activity_info_daycount
(
    `id`             string COMMENT '编号', -- dwd_dim_activity_info
    `activity_name`  string COMMENT '活动名称', -- dwd_dim_activity_info
    `activity_type`  string COMMENT '活动类型', -- dwd_dim_activity_info
    `start_time`     string COMMENT '开始时间', -- dwd_dim_activity_info
    `end_time`       string COMMENT '结束时间', -- dwd_dim_activity_info
    `create_time`    string COMMENT '创建时间', -- dwd_dim_activity_info
    `display_count`  bigint COMMENT '曝光次数', -- dwd_display_log  item_type='activity_id'
    `order_count`    bigint COMMENT '下单次数', -- dwd_fact_order_info activity_id not null
    `order_amount`   decimal(20, 2) COMMENT '下单金额', -- dwd_fact_order_info activity_id not null
        `payment_count`  bigint COMMENT '支付次数',  -- dwd_fact_order_info activity_id not null
    `payment_amount` decimal(20, 2) COMMENT '支付金额'  -- dwd_fact_order_info activity_id not null
) COMMENT '每日活动统计'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_activity_info_daycount/';