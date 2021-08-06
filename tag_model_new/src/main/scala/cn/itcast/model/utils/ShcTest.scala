package cn.itcast.model.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object ShcTest {

  def main(args: Array[String]): Unit = {
    // 1. 读取 HBase 表
    // 1.1. 配置 HBase 表结构, SHC 中配置使用 JSON 的形式来配置的
    // table 指定的是要读取或者要写入的哪一张 HBase 表
    // rowkey 指定 HBase 中 RowKey 所在的列
    // 注意点: 指定了 RowKey 的话, 这个 RowKey 一定要出现在 DataFrame 中, 所以 Columns 中, 一定要配置 RowKey 列
    // columns 指定了要出现在 DataFrame 中的所有列
    // columns 是一个 JSON 对象, 属性名是出现在 DataFrame 中名字, 属性值就是 HBase 中的列
    val catalog =
    s"""
      |{
      |    "table": {
      |        "namespace": "default", "name": "ods_payment_info"
      |    },
      |    "rowkey": "user_id",
      |    "columns": {
      |        "user_id": {"cf": "rowkey", "col": "user_id", "type": "string"},
      |        "alipay_trade_no": {"cf": "default", "col": "alipay_trade_no", "type": "string"}
      |    }
      |}
      |""".stripMargin

    // 1.2. 使用 Spark 连接 HBase
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[*]")
      .getOrCreate()

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    source.show()

    // 2. 写入 HBase 表
    val catalogWrite =
      s"""
         |{
         |    "table": {
         |        "namespace": "default", "name": "tbl_test1"
         |    },
         |    "rowkey": "user_id",
         |    "columns": {
         |        "user_id": {"cf": "rowkey", "col": "user_id", "type": "string"},
         |        "alipay_trade_no": {"cf": "default", "col": "alipay_trade_no", "type": "string"}
         |    }
         |}
         |""".stripMargin

    source.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      创建hbase表时设置预分区region为5
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
