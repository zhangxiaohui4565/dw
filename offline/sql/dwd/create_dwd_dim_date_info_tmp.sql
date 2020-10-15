DROP TABLE IF EXISTS `gmall.dwd_dim_date_info_tmp`;
CREATE EXTERNAL TABLE `gmall.dwd_dim_date_info_tmp`
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
) COMMENT '时间临时表'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_dim_date_info_tmp/';
-- 加载外部文件数据到临时表中  然后再查询临时表 使其存储格式为列式存储
--- load data local inpath '/opt/git/dw/offline/sql/v3/dwd/date_info.txt' into table gmall.dwd_dim_date_info_tmp;