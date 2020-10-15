drop table if exists gmall.dwt_area_topic;
create external table gmall.dwt_area_topic
(
    `id`                      bigint COMMENT '编号',
    `province_name`           string COMMENT '省份名称',
    `area_code`               string COMMENT '地区编码',
    `iso_code`                string COMMENT 'iso编码',
    `region_id`               string COMMENT '地区ID',
    `region_name`             string COMMENT '地区名称',
    `login_day_count`         string COMMENT '当天活跃设备数',
    `login_last_30d_count`    string COMMENT '最近30天活跃设备数',
    `order_day_count`         bigint COMMENT '当天下单次数',
    `order_day_amount`        decimal(16, 2) COMMENT '当天下单金额',
    `order_last_30d_count`    bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount`   decimal(16, 2) COMMENT '最近30天下单金额',
    `payment_day_count`       bigint COMMENT '当天支付次数',
    `payment_day_amount`      decimal(16, 2) COMMENT '当天支付金额',
    `payment_last_30d_count`  bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16, 2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
    stored as parquet
    location '/warehouse/gmall/dwt/dwt_area_topic/';