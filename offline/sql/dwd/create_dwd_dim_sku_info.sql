DROP TABLE IF EXISTS `gmall.dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `gmall.dwd_dim_sku_info`
(
    `id`             string COMMENT '商品id',
    `spu_id`         string COMMENT 'spuid',
    `price`          decimal(16, 2) COMMENT '商品价格',
    `sku_name`       string COMMENT '商品名称',
    `sku_desc`       string COMMENT '商品描述',
    `weight`         decimal(16, 2) COMMENT '重量',
    `tm_id`          string COMMENT '品牌id',
    `tm_name`        string COMMENT '品牌名称',
    `category3_id`   string COMMENT '三级分类id',
    `category2_id`   string COMMENT '二级分类id',
    `category1_id`   string COMMENT '一级分类id',
    `category3_name` string COMMENT '三级分类名称',
    `category2_name` string COMMENT '二级分类名称',
    `category1_name` string COMMENT '一级分类名称',
    `spu_name`       string COMMENT 'spu名称',
    `create_time`    string COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_sku_info/';