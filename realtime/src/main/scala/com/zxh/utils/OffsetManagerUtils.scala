package com.zxh.utils

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable


/**
 * @author zhangxh
 * @date 2020/11/10 8:56
 * @version 1.0
 */
object OffsetManagerUtils {

  private def getEarliestOffsets(kafkaParams: Map[String, Object], topics: List[String]) = {
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //kafka api
    newKafkaParams.toMap.asInstanceOf[java.util.Map[String, Object]]
    val consumer = new KafkaConsumer(newKafkaParams.toMap.asInstanceOf[java.util.Map[String, Object]])
    //订阅
    import scala.collection.JavaConverters._
    val topic = topics.asJava
    consumer.subscribe(topic)
    val noOffsetForPartitionExceptionSet = mutable.Set()
    try
      consumer.poll(Duration.ZERO)
    catch {
      case e: NoOffsetForPartitionException =>
      //        noOffsetForPartitionExceptionSet.add(e.partition())
      //邮件报警
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
    topicOffsetMap
  }

  def main(args: Array[String]): Unit = {
    println("11")
  }
}
