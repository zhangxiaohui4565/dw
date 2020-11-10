package com.zxh.utils

import java.util

import com.zxh.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @author zhangxh
 * @date 2020/11/9 14:47
 * @version 1.0
 */
object XhESUtils {
  private val esUrls: util.List[String] = PropertiesUtil.load("config.properties").getProperty("es.cluster.urls").split(",").toList.asJava
  val factory = new JestClientFactory
  val conf: HttpClientConfig = new HttpClientConfig.Builder(esUrls)
    .multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000)
    .readTimeout(10000)
    .build()
  factory.setHttpClientConfig(conf)

  /**
   * 获取JestClient连接
   *
   * @return
   */
  def getESClient: JestClient = factory.getObject

  /**
   * 插入单条数据
   *
   * @param indexName 索引名称
   * @param source    单条元数据
   */
  def insertSingle(indexName: String, source: Any): Unit = {
    val client = getESClient
    val index = new Index.Builder(source)
      .index(indexName)
      .build()
    try {
      client.execute(index)
    } finally {
      client.close()
    }
  }


  /**
   * 插入多条数据 sources:
   *
   * @param indexName 索引名称前缀
   * @param sources   元数据集合
   */
  def batchSaveToES(indexName: String, sources: List[DauInfo]): Unit = {
    if (sources.isEmpty) {
      return
    }
    val client = getESClient
    var indexDt = ""
    var bulkBuilder = new Bulk.Builder()
      .defaultIndex(indexName)
    // 先按照时间进行排序，避免数据插入到不同的索引中去
    sources.sortBy(_.dt).foreach {
      data => {
        if (indexDt.equals("")) {
          indexDt = data.dt
          bulkBuilder = new Bulk.Builder()
            .defaultIndex(indexName + indexDt)
          bulkBuilder.addAction(new Index.Builder(data).id(data.mid).build())
        }else if (data.dt.equals(indexDt) ){
          bulkBuilder.addAction(new Index.Builder(data).id(data.mid).build())
        }else{
          client.execute(bulkBuilder.build())
          indexDt = data.dt
          bulkBuilder = new Bulk.Builder()
            .defaultIndex(indexName + indexDt)
        }



      }
    }
    // 将最后一个批次插入
    try {
      client.execute(bulkBuilder.build())
    } finally {
      closeClient(client)
    }
  }

  /**
   * 关闭客户端
   *
   * @param client
   */
  def closeClient(client: JestClient): Unit = {
    if (client != null) try client.close() catch {
      case e => e.printStackTrace()
    }
  }
}
