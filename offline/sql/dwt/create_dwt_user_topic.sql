drop table if exists gmall.dwt_user_topic;
create external table gmall.dwt_user_topic
(
    user_id                 string comment '用户id',
    login_date_first        string comment '首次登录时间',
    login_date_last         string comment '末次登录时间',
    login_count             bigint comment '累积登录天数',
    login_last_30d_count    bigint comment '最近30日登录天数',
    order_date_first        string comment '首次下单时间',
    order_date_last         string comment '末次下单时间',
    order_count             bigint comment '累积下单次数',
    order_amount            decimal(16, 2) comment '累积下单金额',
    order_last_30d_count    bigint comment '最近30日下单次数', -- group by  user_id 最近三十天的
    order_last_30d_amount   bigint comment '最近30日下单金额',
    payment_date_first      string comment '首次支付时间',
    payment_date_last       string comment '末次支付时间',
    payment_count           decimal(16, 2) comment '累积支付次数',
    payment_amount          decimal(16, 2) comment '累积支付金额',
    payment_last_30d_count  decimal(16, 2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16, 2) comment '最近30日支付金额'
) COMMENT '会员主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_user_topic/';


select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2020-10-04',old.login_date_first),
    if(new.login_count>0,'2020-10-04',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2020-10-04',old.order_date_first),
    if(new.order_count>0,'2020-10-04',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2020-10-04',old.payment_date_first),
    if(new.payment_count>0,'2020-10-04',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
    gmall.dwt_user_topic old
        full outer join
    (
        select
            user_id,
            sum(if(dt='2020-10-04',login_count,0)) login_count,
            sum(if(dt='2020-10-04',order_count,0)) order_count,
            sum(if(dt='2020-10-04',order_amount,0)) order_amount,
            sum(if(dt='2020-10-04',payment_count,0)) payment_count,
            sum(if(dt='2020-10-04',payment_amount,0)) payment_amount,
            sum(if(login_count>0,1,0)) login_last_30d_count,
            sum(order_count) order_last_30d_count,
            sum(order_amount) order_last_30d_amount,
            sum(payment_count) payment_last_30d_count,
            sum(payment_amount) payment_last_30d_amount
        from gmall.dws_user_action_daycount
        where dt>=date_add( '2020-10-04',-30)
        group by user_id
    )new
    on old.user_id=new.user_id;