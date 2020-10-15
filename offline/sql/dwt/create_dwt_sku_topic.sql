drop table if exists gmall.dwt_sku_topic;
create external table gmall.dwt_sku_topic
(
    sku_id                          string comment 'sku_id',
    spu_id                          string comment 'spu_id',
    order_last_30d_count            bigint comment '最近30日被下单次数',
    order_last_30d_num              bigint comment '最近30日被下单件数',
    order_last_30d_amount           decimal(16, 2) comment '最近30日被下单金额',
    order_count                     bigint comment '累积被下单次数',
    order_num                       bigint comment '累积被下单件数',
    order_amount                    decimal(16, 2) comment '累积被下单金额',
    payment_last_30d_count          bigint comment '最近30日被支付次数',
    payment_last_30d_num            bigint comment '最近30日被支付件数',
    payment_last_30d_amount         decimal(16, 2) comment '最近30日被支付金额',
    payment_count                   bigint comment '累积被支付次数',
    payment_num                     bigint comment '累积被支付件数',
    payment_amount                  decimal(16, 2) comment '累积被支付金额',
    refund_last_30d_count           bigint comment '最近三十日退款次数',
    refund_last_30d_num             bigint comment '最近三十日退款件数',
    refund_last_30d_amount          decimal(16, 2) comment '最近三十日退款金额',
    refund_count                    bigint comment '累积退款次数',
    refund_num                      bigint comment '累积退款件数',
    refund_amount                   decimal(16, 2) comment '累积退款金额',
    cart_last_30d_count             bigint comment '最近30日被加入购物车次数',
    cart_count                      bigint comment '累积被加入购物车次数',
    favor_last_30d_count            bigint comment '最近30日被收藏次数',
    favor_count                     bigint comment '累积被收藏次数',
    appraise_last_30d_good_count    bigint comment '最近30日好评数',
    appraise_last_30d_mid_count     bigint comment '最近30日中评数',
    appraise_last_30d_bad_count     bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count             bigint comment '累积好评数',
    appraise_mid_count              bigint comment '累积中评数',
    appraise_bad_count              bigint comment '累积差评数',
    appraise_default_count          bigint comment '累积默认评价数'
) COMMENT '商品主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_sku_topic/';