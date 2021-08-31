package cn.itcast.model.utils

import cn.itcast.model.{HBaseCatalog, HBaseColumn, HBaseTable}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

//noinspection DuplicatedCode
object ShcUtils {
  val HBASE_ROWKEY_FIELD = "id"
  val HBASE_COLUMN_DEFAULT_TYPE = "string"
  val HBASE_NAMESPACE = "default"
  val HBASE_DEFAULT_COLUMN_FAMILY = "default"
  val HBASE_USER_PROFILE = "user_profile"

  /**
   * 给定读取参数, 读取 HBase 的数据
   */
  def read(tableName: String, inFields: Array[String], columnFamily: String, spark: SparkSession): DataFrame = {
    // 1. 处理 Columns 对象
    //case class HBaseColumn(cf: String, col: String, `type`: String)
    /*
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
      |"""
     */
    val columns = mutable.HashMap.empty[String, HBaseColumn]
    columns += HBASE_ROWKEY_FIELD -> HBaseColumn("rowkey", HBASE_ROWKEY_FIELD, HBASE_COLUMN_DEFAULT_TYPE)
    for (field <- inFields) {
      columns += field -> HBaseColumn(columnFamily, field, HBASE_COLUMN_DEFAULT_TYPE)
    }

    // 2. 处理 HBaseTable 对象
    //case class HBaseTable(namespace: String, name: String)
    val table = HBaseTable(HBASE_NAMESPACE, tableName)

    // 3. 拼装 Catalog 对象
    val catalog = HBaseCatalog(table, HBASE_ROWKEY_FIELD, columns.toMap)

    // 4. 把 Catalog 对象转为 JSON 字符串形式
    val catalogJson = objectToJson(catalog)

    // 5. 读取
    spark.read
      .option(HBaseTableCatalog.tableCatalog, catalogJson)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  /**
   * 写入数据
   */
  def writeToHBase(outFields: Array[String], dataFrame: DataFrame, regionCount: String): Unit = {
    val columns = mutable.HashMap.empty[String, HBaseColumn]
    columns += HBASE_ROWKEY_FIELD -> HBaseColumn("rowkey", HBASE_ROWKEY_FIELD, HBASE_COLUMN_DEFAULT_TYPE)

    for (field <- outFields) {
      columns += field -> HBaseColumn(HBASE_DEFAULT_COLUMN_FAMILY, field, HBASE_COLUMN_DEFAULT_TYPE)
    }

    val table = HBaseTable(HBASE_NAMESPACE, HBASE_USER_PROFILE)
    val catalog = HBaseCatalog(table, HBASE_ROWKEY_FIELD, columns.toMap)

    // 1.2. 转换 Catalog 为 JSON
    val catalogJson = objectToJson(catalog)

    // 2. 存入数据, 访问 HBase
    dataFrame.write
      .option(HBaseTableCatalog.tableCatalog, catalogJson)
      .option(HBaseTableCatalog.newTable, regionCount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  /**
   * 把 对象转为 JSON 字符串形式
   *
   * @param obj 对象
   * @return JSON 字符串
   */
  def objectToJson(obj: AnyRef): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    write(obj)
  }
}
