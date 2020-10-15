DROP TABLE IF EXISTS `gmall.dwd_dim_date_info`;
CREATE EXTERNAL TABLE `gmall.dwd_dim_date_info`
(
    `date_id`    string COMMENT '日',
    `week_id`    string COMMENT '周',
    `week_day`   string COMMENT '周的第几天',
    `day`        string COMMENT '每月的第几天',
    `month`      string COMMENT '第几月',
    `quarter`    string COMMENT '第几季度',
    `year`       string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_date_info/';