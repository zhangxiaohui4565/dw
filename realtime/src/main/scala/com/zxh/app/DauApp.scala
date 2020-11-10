package com.zxh.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zxh.bean.DauInfo
import com.zxh.utils.{RedisUtil, XhESUtils, XhKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * @author zhangxh
 * @date 2020/11/6 14:46
 * @version 1.0
 */
object DauApp {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.WARN)
    val dauAppConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(dauAppConf, Seconds(5))
    val groupId = "GMALL_DAU_CONSUMER24"
    val topic = "GMALL_START"
    val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = XhKafkaUtil.getKafkaStream(topic, ssc, groupId)

    // 进行日志格式添加
    val jsonObjDStream: DStream[JSONObject] = startupInputDstream.map { record =>
      //获取启动日志
      val jsonStr: String = record.value()
      //将启动日志转换为json对象
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      //获取时间戳 毫秒数
      val ts: Long = jsonObj.getLong("ts")
      //获取字符串   日期 小时
      val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      //对字符串日期和小时进行分割，分割后放到json对象中，方便后续处理
      val dateHour: Array[String] = dateHourString.split(" ")
      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))
      jsonObj
    }
    //    jsonObjDStream.print(1)
    // 进行redis 数据验证 dau:2019-01-22	设备id
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //获取Redis客户端
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //定义当前分区过滤后的数据
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        for (jsonObj <- jsonObjItr) {
          //获取当前日期
          val dt: String = jsonObj.getString("dt")
          //获取设备mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接向Redis放的数据的key
          val dauKey: String = "dau:" + dt
          //判断Redis中是否存在该数据
          val isNew: Long = jedisClient.sadd(dauKey, mid)
          //设置当天的key数据失效时间为24小时
          jedisClient.expire(dauKey, 3600 * 24)

          if (isNew == 1L) {
            filteredList.append(jsonObj)

          }
        }

        jedisClient.close()
        filteredList.toIterator
      }
    }
    //    filteredDStream.count().print()
    filteredDStream.foreachRDD {

      rdd => { //获取DS中的RDD
        rdd.foreachPartition { //以分区为单位对RDD中的数据进行处理，方便批量插入
          jsonItr => {
            val dauList: List[DauInfo] = jsonItr.map {
              jsonObj => {
                //每次处理的是一个json对象   将json对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认00
                  jsonObj.getLong("ts")
                )
              }
            }.toList
            //对分区的数据进行批量处理


            XhESUtils.batchSaveToES("gmall2020_dau_info_", dauList)
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
