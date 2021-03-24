package com.zxh.sql

/**
 * Flinksql使用DDL的方式，读取kafka数据，进行数据实时处理，写结果到mysql表中
 * @author zhangxh
 * @date 2021/3/24 11:46
 * @version 1.0
 */
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.log4j.{Level, Logger}
import org.apache.flink.api.scala._

/**
 * todo: Flinksql使用DDL的方式，读取kafka数据，进行数据实时处理，写结果到mysql表中
 */
object FlinkSqlConnectKafka {

  //Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //todo:1、构建流处理环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    //todo:2、构建StreamTableEnvironment
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() // 使用 blink planner
      .inStreamingMode() // 流处理模式
      .build()

    val streamTableEnvironment = StreamTableEnvironment.create(streamEnv, bsSettings)


    //todo: 3、通过DDL，注册kafka数据源表
    streamTableEnvironment.executeSql(SqlContext.kafkaSourceSql)

    //todo: 4、执行查询任务
    val result: Table = streamTableEnvironment.sqlQuery("select * from kafkaTable")

    //todo: 5、table转换成流
    val dataStream: DataStream[Row] = streamTableEnvironment.toAppendStream[Row](result)

    dataStream.print("数据流结果：")


    //todo: 6、通过DDL，注册mysql sink表
    streamTableEnvironment.executeSql(SqlContext.mysqlSinkSql)


    //todo: 7、计算指标落地到mysql表
    streamTableEnvironment.executeSql(SqlContext.pvuvSql)


    //todo: 8、启动任务
    streamEnv.execute("FlinkSqlConnectKafka")


  }
}

