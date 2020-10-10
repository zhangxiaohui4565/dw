#!/bin/bash

APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    dt=$2
else
    dt=`date -d "-1 day" +%F`
fi

sql1="
load data inpath '/origin_data/$APP/db/order_info/$dt' OVERWRITE into table ${APP}.ods_order_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/order_detail/$dt' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$dt');

load data inpath '/origin_data/$APP/db/sku_info/$dt' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/user_info/$dt' OVERWRITE into table ${APP}.ods_user_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/payment_info/$dt' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/base_category1/$dt' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$dt');

load data inpath '/origin_data/$APP/db/base_category2/$dt' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$dt');

load data inpath '/origin_data/$APP/db/base_category3/$dt' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$dt');

load data inpath '/origin_data/$APP/db/base_trademark/$dt' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$dt');

load data inpath '/origin_data/$APP/db/activity_info/$dt' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/activity_order/$dt' OVERWRITE into table ${APP}.ods_activity_order partition(dt='$dt');

load data inpath '/origin_data/$APP/db/cart_info/$dt' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/comment_info/$dt' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/coupon_info/$dt' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/coupon_use/$dt' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$dt');

load data inpath '/origin_data/$APP/db/favor_info/$dt' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/order_refund_info/$dt' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/order_status_log/$dt' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$dt');

load data inpath '/origin_data/$APP/db/spu_info/$dt' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$dt');

load data inpath '/origin_data/$APP/db/activity_rule/$dt' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$dt');

load data inpath '/origin_data/$APP/db/base_dic/$dt' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$dt');
"

sql2="
load data inpath '/origin_data/$APP/db/base_province/$dt' OVERWRITE into table ${APP}.ods_base_province;

load data inpath '/origin_data/$APP/db/base_region/$dt' OVERWRITE into table ${APP}.ods_base_region;
"
case $1 in
"first"){
    /usr/bin/beeline -u "jdbc:hive2://node01:10000/" -n hive -p hive -e  "$sql1$sql2"
};;
"all"){
    /usr/bin/beeline -u "jdbc:hive2://node01:10000/" -n hive -p hive -e  "$sql1"
};;
esac