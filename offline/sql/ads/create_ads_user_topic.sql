drop table if exists gmall.ads_user_topic;
create external table gmall.ads_user_topic
(
    `dt`                    string COMMENT '统计日期',
    `day_users`             string COMMENT '活跃会员数',
    `day_new_users`         string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users`         string COMMENT '总付费会员数',
    `users`                 string COMMENT '总会员数',
    `day_users2users`       decimal(16, 2) COMMENT '会员活跃率',
    `payment_users2users`   decimal(16, 2) COMMENT '会员付费率',
    `day_new_users2users`   decimal(16, 2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_topic';