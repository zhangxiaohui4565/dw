package com.zxh.utils

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * @author zhangxh
 * @date 2020/11/10 8:56
 * @version 1.0
 */
object OffsetManagerUtils {

  // 基础kafka 配置
  private val properties: Properties =
    PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaParam = collection.mutable.Map[String, Object](
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "GMALL_CONSUMER",
    "value.deserializer" -> classOf[StringDeserializer]
  )

  /**
   * 获取最早偏移量
   *
   * @param kafkaParams kafka 配置参数
   * @param topics      主题
   * @return
   */
  def getEarliestOffsets(kafkaParams: collection.mutable.Map[String, Object], topics: List[String]) = {
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val map: Map[String, Object] = newKafkaParams.toMap

    //kafka api
    val consumer = new KafkaConsumer(map.asJava)
    //订阅

    val topic = topics.asJava
    consumer.subscribe(topic)
    try
      // 此处需要注意，没有数据时会阻塞一秒，有数据时，会立刻返回偏移量 或者使用过时的方法，传入毫秒数
      consumer.poll(1)
    catch {
      case e: NoOffsetForPartitionException =>
        //        noOffsetForPartitionExceptionSet.add(e.partition())
        //邮件报警
        println(e)
    }
    val partitions: util.Set[TopicPartition] = consumer.assignment()
    //获取 分区信息
    val topicp = partitions
    //暂定消费
    consumer.pause(topicp)
    //从头开始
    consumer.seekToBeginning(topicp)
    // key 为new TopicPartition(topic.partition)
    //position(TopicPartition partition)
    val topicOffsetMap = mutable.Map[TopicPartition, Long]()
    for (tp <- topicp.asScala) {
      // Get the offset of the <i>next record</i>
      val pOffset: Long = consumer.position(tp)
      topicOffsetMap(tp) = pOffset
    }
    consumer.unsubscribe()
    consumer.close()
    topicOffsetMap.toMap
  }

  def getEarliestOffsets(topics: List[String]): Map[TopicPartition, Long] = {
    this.getEarliestOffsets(kafkaParam, topics)
  }

  def getEarliestOffsets(topic: String): Map[TopicPartition, Long] = {
    this.getEarliestOffsets(kafkaParam, List(topic))
  }


  /**
   * 获取指定topic 和groupid 下的分区和偏移量数据
   *
   * @param groupId 消费者组
   * @param topic   主题
   * @return
   */
  def getConsumeOffset(groupId: String, topic: String): Map[TopicPartition, Long] = {
    val redisClient: Jedis = RedisUtil.getJedisClient
    // 存储偏移量 到redis 中，offset:topic：group`
    var offsetKey = s"offset:${topic}:${groupId}"
    val offsetMap: mutable.Map[String, String] = redisClient.hgetAll(offsetKey).asScala
    redisClient.close()
    // 获取最早偏移量进行偏移量校验
    val earlistPartitionOffset: Map[TopicPartition, Long] = this.getEarliestOffsets(topic)
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map {
      case (partitionId, offset) => {
        println("redis存储分区偏移量：" + partitionId + ":" + offset.toLong)
        //将Redis中保存的分区对应的偏移量进行封装
        val earlistOffset: Long = earlistPartitionOffset.get(new TopicPartition(topic, partitionId.toInt)).get
        if (earlistOffset > offset.toLong) {
          println("矫正偏移量")
          println("读取分区偏移量：" + partitionId + ":" + earlistOffset)
          (new TopicPartition(topic, partitionId.toInt), earlistOffset)
        } else {
          println("读取分区偏移量：" + partitionId + ":" + offset.toLong)
          (new TopicPartition(topic, partitionId.toInt), offset.toLong)
        }
      }
    }.toMap
    kafkaOffsetMap
  }

  /**
   * 保存最新数据的偏移量
   * @param topicName 主题名称
   * @param groupId 消费者组
   * @param offsetRanges 存有偏移量的rdd
   */
  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //定义Java的map集合，用于向Reids中保存数据
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    //对封装的偏移量数组offsetRanges进行遍历
    for (offset <- offsetRanges) {
      //获取分区
      val partition: Int = offset.partition
      //获取结束点
      val untilOffset: Long = offset.untilOffset
      //封装到Map集合中
      offsetMap.put(partition.toString, untilOffset.toString)
      //打印测试
      println("保存分区:" + partition + ":" + offset.fromOffset + "--->" + offset.untilOffset)
    }
    //拼接Reids中存储偏移量的key
    val offsetKey: String = "offset:" + topicName + ":" + groupId
    //如果需要保存的偏移量不为空 执行保存操作
    if (offsetMap != null && offsetMap.size() > 0) {
      //获取Redis客户端
      val jedis: Jedis = RedisUtil.getJedisClient
      //保存到Redis中
      jedis.hmset(offsetKey, offsetMap)
      //关闭客户端
      jedis.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(this.getConsumeOffset("GMALL_DAU_CONSUMER12","GMALL_START"))
  }
}
