drop table if exists gmall.dwt_uv_topic;
create external table gmall.dwt_uv_topic
(
    `mid_id`           string comment '设备id',
    `brand`            string comment '手机品牌',
    `model`            string comment '手机型号',
    `login_date_first` string comment '首次活跃时间',
    `login_date_last`  string comment '末次活跃时间',
    `login_day_count`  bigint comment '当日活跃次数',
    `login_count`      bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_uv_topic';





