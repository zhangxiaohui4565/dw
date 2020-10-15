drop table if exists gmall.dws_area_stats_daycount;
create external table gmall.dws_area_stats_daycount
(
    `id`             bigint COMMENT '编号', -- dwd_dim_base_province
    `province_name`  string COMMENT '省份名称', -- dwd_dim_base_province
    `area_code`      string COMMENT '地区编码',  --dwd_dim_base_province
    `iso_code`       string COMMENT 'iso编码', -- dwd_dim_base_province
    `region_id`      string COMMENT '地区ID', -- dwd_dim_base_province
    `region_name`    string COMMENT '地区名称', -- dwd_dim_base_province
    `login_count`    string COMMENT '活跃设备数', -- dwd_start_log
    `order_count`    bigint COMMENT '下单次数', -- dwd_fact_order_info
    `order_amount`   decimal(20, 2) COMMENT '下单金额', -- dwd_fact_order_info
    `payment_count`  bigint COMMENT '支付次数', -- dwd_fact_order_info
    `payment_amount` decimal(20, 2) COMMENT '支付金额' -- dwd_fact_order_info
) COMMENT '每日地区统计表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_area_stats_daycount/';