package com.zxh.demo
import junit.framework.Test
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient.KuduClientBuilder
/**
 * @author zhangxh
 * @date 2021/2/24 14:22
 * @version 1.0
 */
object Test {

  def main(args: Array[String]): Unit = {
//    createTable()
//    insertData()
    scan()
  }

//  @Test
  def createTable(): Unit = {
    // Kudu 的 Master 地址
    val KUDU_MASTER = "node01:7051"

    // 创建 KuduClient 入口
    val kuduClient = new KuduClientBuilder(KUDU_MASTER).build()

    // 创建列定义的 List
    val columns = List(
      new ColumnSchemaBuilder("key", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("value", Type.STRING).key(false).build()
    )

    // 因为是 Java 的 API, 所以在使用 List 的时候要转为 Java 中的 List
    import scala.collection.JavaConverters._
    val javaColumns = columns.asJava

    // 创建 Schema
    val schema = new Schema(javaColumns)

    // 因为 Kudu 必须要指定分区, 所以先创建一个分区键设置
    val keys = List("key").asJava
    val options = new CreateTableOptions().setRangePartitionColumns(keys).setNumReplicas(1)

    // 通过 Schema 创建表
    kuduClient.createTable("simple", schema, options)


  }
  def insertData(): Unit = {
    val KUDU_MASTER = "node01:7051"
    val kuduClient = new KuduClientBuilder(KUDU_MASTER).build()

    // 得到表对象, 表示对一个表进行操作
    val table = kuduClient.openTable("simple")

    // 表示操作, 组织要操作的行数据
    val insert = table.newInsert()
    val row = insert.getRow
    row.addString(0, "A")
    row.addString(1, "1")

    // 开启会话开始操作
    val session = kuduClient.newSession()
    session.apply(insert)
  }

  def scan(): Unit = {
    val KUDU_MASTER = "node01:7051"
    val kuduClient = new KuduClientBuilder(KUDU_MASTER).build()

    // 1. 设置投影
    import scala.collection.JavaConverters._
    val projects = List("key", "value").asJava

    // 2. scanner 租装
    val scanner = kuduClient.newScannerBuilder(kuduClient.openTable("simple"))
      .setProjectedColumnNames(projects)
      .build()

    // 3. 获取结果
    while (scanner.hasMoreRows) {
      // 这个 Results 是一整个 tablet
      val results = scanner.nextRows()

      while (results.hasNext) {
        // 这才是一条具体的数据
        val result = results.next()

        println(result.getString(0), result.getString(1))
      }
    }
  }
}
