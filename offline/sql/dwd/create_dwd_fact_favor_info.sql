drop table if exists gmall.dwd_fact_favor_info;
create external table gmall.dwd_fact_favor_info
(
    `id`          string COMMENT '编号',
    `user_id`     string COMMENT '用户id',
    `sku_id`      string COMMENT 'skuid',
    `spu_id`      string COMMENT 'spuid',
    `is_cancel`   string COMMENT '是否取消',
    `create_time` string COMMENT '收藏时间',
    `cancel_time` string COMMENT '取消时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_favor_info/';