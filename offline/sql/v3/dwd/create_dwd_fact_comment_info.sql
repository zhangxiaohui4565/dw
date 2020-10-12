drop table if exists gmall.dwd_fact_comment_info;
create external table gmall.dwd_fact_comment_info
(
    `id`          string COMMENT '编号',
    `user_id`     string COMMENT '用户ID',
    `sku_id`      string COMMENT '商品sku',
    `spu_id`      string COMMENT '商品spu',
    `order_id`    string COMMENT '订单ID',
    `appraise`    string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '评价事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_comment_info/';