-- 用户维度
drop table if exists gmall.dws_user_action_daycount;
create external table gmall.dws_user_action_daycount
(
    user_id            string comment '用户 id',  --dwd_start_log
    login_count        bigint comment '登录次数', --dwd_start_log
    cart_count         bigint comment '加入购物车次数', --dwd_action_log action_id='cart_add'
    order_count        bigint comment '下单次数',   -- dwd_fact_order_info
    order_amount       decimal(16, 2) comment '下单金额',  -- dwd_fact_order_info
    payment_count      bigint comment '支付次数', -- dwd_fact_payment_info
    payment_amount     decimal(16, 2) comment '支付金额', -- dwd_fact_payment_info
    order_detail_stats array<struct<sku_id :string,sku_num :bigint,order_count :bigint,order_amount
                                    :decimal(20, 2)>> comment '下单明细统计' -- dwd_fact_order_detail
) COMMENT '每日会员行为'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_user_action_daycount/';


