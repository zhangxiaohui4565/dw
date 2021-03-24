package com.zxh.sql

object SqlContext {

  //todo: 连接kafka构建源表的sql语句

    lazy val kafkaSourceSql =
      """
        |create table kafkaTable
        |(
        |    user_id BIGINT,
        |    item_id BIGINT,
        |    category_id BIGINT,
        |    behavior STRING,
        |    ts TIMESTAMP
        |) with (
        |	'connector' = 'kafka',                                                      -- 使用 kafka connector
        |	'topic' = 'user_behavior',                                                  -- kafka topic
        |	'properties.group.id' = 'testGroup',                                        -- kafka consumer group消费者组
        |	'properties.bootstrap.servers' = 'node01:19091,node02:19091,node03:19091',     -- kafka 集群地址
        |	'scan.startup.mode' = 'latest-offset',                                      -- 从最新 offset 开始读取
        |	'format' = 'json')                                                          -- 数据源格式为 json
      """.stripMargin


  //todo: 定义要输出的表的sql语句
  lazy val mysqlSinkSql  =
    """
      |CREATE TABLE pvuv(
      |    dt VARCHAR,
      |    pv BIGINT,
      |    uv BIGINT,
      |    PRIMARY KEY (`dt`) NOT ENFORCED
      |) WITH (
      |    'connector' = 'jdbc',                                               -- 使用 jdbc connector
      |    'url' = 'jdbc:mysql://node03:3306/flink?useSSL=false',              -- mysql jdbc url
      |    'table-name' = 'pvuv',                                              -- 表名
      |    'username' = 'root',                                                -- 用户名
      |    'password' = 'Viy3M9UCCNNS0AmE',                                              -- 密码
      |    'sink.buffer-flush.max-rows' = '10',                                --数据量达到10条刷数据到mysql
      |    'sink.buffer-flush.max-rows' = '1'                                  --时间达到1秒刷数据到mysql
      |)
    """.stripMargin


  //todo: 业务分析处理的sql，求取pv和uv
  lazy val pvuvSql  =
    """
      |INSERT INTO pvuv
      |SELECT
      |  DATE_FORMAT(ts, 'yyyy-MM-dd') dt,
      |  COUNT(*) AS pv,
      |  COUNT(DISTINCT user_id) AS uv
      |FROM kafkaTable
      |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd')
    """.stripMargin

}