#!/bin/bash

# 定义变量方便修改
APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   dt=$1
else
   dt=`date -d "-1 day" +%F`
fi

echo ================== 日志日期为 $dt ==================
sql="
load data inpath '/origin_data/$APP/log/topic_log/$dt' into table ${APP}.ods_log partition(dt='$dt');
"

/usr/bin/beeline -u "jdbc:hive2://node01:10000/" -n hive -p hive -e "$sql"