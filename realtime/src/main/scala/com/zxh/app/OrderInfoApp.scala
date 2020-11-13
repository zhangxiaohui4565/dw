package com.zxh.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zxh.bean.{OrderInfo, UserStatus}
import com.zxh.utils.{OffsetManagerUtils, PhoenixUtil, XhKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Author: Felix
  * Desc: 判断是否为首单用户实现
  */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {

    //1.从Kafka中查询订单信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group12"

    //从Redis中读取Kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtils.getConsumeOffset(topic,groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null

    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      //Redis中有偏移量  根据Redis中保存的偏移量读取
      recordDstream = XhKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
      // Redis中没有保存偏移量  Kafka默认从最新读取
      recordDstream = XhKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从Kafka中读取数据之后，直接就获取了偏移量的位置，因为KafkaRDD可以转换为HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对从Kafka中读取到的数据进行结构转换，由Kafka的ConsumerRecord转换为一个OrderInfo对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
        //通过对创建时间2020-07-13 01:38:16进行拆分，赋值给日期和小时属性，方便后续处理
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋给小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
    /*
    //方案1：对DStream中的数据进行处理，判断下单的用户是否为首单
    //缺点：每条订单数据都要执行一次SQL，SQL执行过于频繁
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //通过phoenix工具到hbase中查询用户状态
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id ='${orderInfo.user_id}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
    orderInfoWithFirstFlagDStream.print(1000)
    */
    //方案2：对DStream中的数据进行处理，判断下单的用户是否为首单
    //优化:以分区为单位，将一个分区的查询操作改为一条SQL
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区内的用户ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从hbase中查询整个分区的用户是否消费过，获取消费过的用户ids
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id in('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已消费过的用户的id集合
        val cosumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))

        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
//注意：orderInfo中user_id是Long类型，一定别忘了进行转换
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            //如已消费过的用户的id集合包含当前下订单的用户，说明不是首单
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    import org.apache.phoenix.spark._
    orderInfoWithFirstFlagDStream.foreachRDD{
      rdd=>{
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order=="1")
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS2020",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration,
          Some("node01,node02,node03:2181")
        )
        //保存偏移量到Redis
        OffsetManagerUtils.saveOffset(topic,groupId,offsetRanges)
      }
    }
    orderInfoWithFirstFlagDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}