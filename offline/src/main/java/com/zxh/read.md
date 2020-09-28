#自定义函数相关备忘
## 注册自定义函数（永久）
create function base_analizer as 'com.zxh.udf.FieldAnalysisUDF' using jar 'hdfs://node01:8020/user/hive/jars/offline-1.0-SNAPSHOT.jar';
create function flat_analizer as 'com.zxh.udtf.EventJsonUDTF' using jar 'hdfs://node01:8020/user/hive/jars/offline-1.0-SNAPSHOT.jar';
## 删除注册函数
drop  function base_analizer;
drop  function flat_analizer;