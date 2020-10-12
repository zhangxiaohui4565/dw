drop table if exists gmall.dwd_dim_user_info_his;
create external table gmall.dwd_dim_user_info_his
(
    `id`           string COMMENT '用户id',
    `name`         string COMMENT '姓名',
    `birthday`     string COMMENT '生日',
    `gender`       string COMMENT '性别',
    `email`        string COMMENT '邮箱',
    `user_level`   string COMMENT '用户等级',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`   string COMMENT '有效开始日期',
    `end_date`     string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_user_info_his/';