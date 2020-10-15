drop table if exists gmall.dwt_activity_topic;
create external table gmall.dwt_activity_topic
(
    `id`                 string COMMENT '编号',
    `activity_name`      string COMMENT '活动名称',
    `activity_type`      string COMMENT '活动类型',
    `start_time`         string COMMENT '开始时间',
    `end_time`           string COMMENT '结束时间',
    `create_time`        string COMMENT '创建时间',
    `display_day_count`  bigint COMMENT '当日曝光次数',
    `order_day_count`    bigint COMMENT '当日下单次数',
    `order_day_amount`   decimal(20, 2) COMMENT '当日下单金额',
    `payment_day_count`  bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20, 2) COMMENT '当日支付金额',
    `display_count`      bigint COMMENT '累积曝光次数',
    `order_count`        bigint COMMENT '累积下单次数',
    `order_amount`       decimal(20, 2) COMMENT '累积下单金额',
    `payment_count`      bigint COMMENT '累积支付次数',
    `payment_amount`     decimal(20, 2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_activity_topic/';