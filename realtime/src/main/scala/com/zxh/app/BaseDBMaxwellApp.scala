package com.zxh.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zxh.utils.{MyKafkaSink, OffsetManagerUtils, XhKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author: Felix
 * Desc: 基于Maxwell从Kafka中读取业务数据，进行分流
 */
object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "gmall2020_db_m"
    val groupId = "base_db_maxwell_group"

    //从Redis中读取偏移量
    var recoredDStream: InputDStream[ConsumerRecord[String, String]] = null
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtils.getConsumeOffset(topic, groupId)
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recoredDStream = XhKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recoredDStream = XhKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前采集周期中处理的数据 对应的分区已经偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recoredDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //将从kafka中读取到的recore数据进行封装为json对象
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取value部分的json字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }

    //从json对象中获取table和data，发送到不同的kafka主题
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            val opType: String = jsonObj.getString("type")
            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
            if (dataJsonObj != null && !dataJsonObj.isEmpty && !"delete".equals(opType)) {
              //获取更新的表名
              val tableName: String = jsonObj.getString("table")
              //拼接发送的主题
              var sendTopic = "ods_" + tableName
              //向kafka发送消息
              MyKafkaSink.send(sendTopic, dataJsonObj.toString())
            }
          }
        }
        //修改Redis中Kafka的偏移量
        OffsetManagerUtils.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}