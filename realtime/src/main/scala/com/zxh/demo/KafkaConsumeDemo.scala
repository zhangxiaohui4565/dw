package com.zxh.demo

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zxh.utils.{RedisUtil, XhKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author zhangxh
  * @date 2020/8/14 10:27
  * @version 1.0
  */
object KafkaConsumeDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_DAU_CONSUMER"
    val topic = "GMALL_START"
    val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = XhKafkaUtil.getKafkaStream(topic, ssc)


    val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
      val startupJson: String = record.value()
      val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
      val ts: Long = startupJSONObj.getLong("ts")
      startupJSONObj
    }

    startLogInfoDStream.print(1)

    /**
      * 将数据输出到redis
      */
    val dauLoginfoDstream: DStream[JSONObject] = startLogInfoDStream.transform { rdd =>
      println("前：" + rdd.count())
      val logInfoRdd: RDD[JSONObject] = rdd.mapPartitions { startLogInfoItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val dauLogInfoList = new ListBuffer[JSONObject]
        val startLogList: List[JSONObject] = startLogInfoItr.toList
        var dauKey = ""
        for (startupJSONObj <- startLogList) {
          /**
            * 数据量少的可以使用这个方式，使用set 方式，数据量大的使用redis string 并设定24小时失效时间
            */
          val ts: Long = startupJSONObj.getLong("ts")
          val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
          dauKey = "dau:" + dt
          val ifFirst: Long = jedis.sadd(dauKey, startupJSONObj.getJSONObject("common").getString("mid"))

          if (ifFirst == 1L) {
            dauLogInfoList += startupJSONObj
          }
        }
        if (dauKey != "") {
          jedis.expire(dauKey, 60)
        }

        jedis.close()
        dauLogInfoList.toIterator
      }
      // println("后：" + logInfoRdd.count())
      logInfoRdd
    }
    dauLoginfoDstream.print(100)
    ssc.start()
    ssc.awaitTermination()


  }


}
