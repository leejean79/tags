package cn.itcast.model.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Hive2HBase {
  val defaultHBaseCF = "default"
  val hfilePath = "/user/admin/data_extra/hfile_cache"
  val hbaseDefaultNameSpace = "default"

  /**
   * 要加载五张表, tbl_goods, tbl_goods_new, tbl_logs, tbl_orders, tbl_users
   * 这五张表都需要处理, 所以, 可能要写五个程序去抽取
   * 优化点1: 通过 args 传入表名, 和相应的表中的主键
   *
   * @param args db.table 1. 数据库名, 2. 表名, 3. 表中的主键列名
   */
  def main(args: Array[String]): Unit = {
    // 校验 参数
    // spark-submit --class --master jarPath arg1 arg2 arg3
    if (args.length < 3) {
      return
    }

    val sourceDbName = args(0)
    val sourceTableName = args(1)
    val sourceKeyFieldName = args(2)

    // 1. 创建配置
    val hadoopConfig = HBaseConfiguration.create()
    // 1.1. 配置: HBase 中落地的那张表的表名, Output TableName
    hadoopConfig.set(TableOutputFormat.OUTPUT_TABLE, sourceTableName)
    // 1.2. 配置: HFile 中写入时的表名
    hadoopConfig.set("hbase.mapreduce.hfileoutputformat.table.name", sourceTableName)

    // 无论是 Map 还是 Reduce 其实处理的都是 KV
    // 1.3. Job配置: 任务执行的信息, Key 的类型
    val job = Job.getInstance(hadoopConfig)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    // 1.4. Job配置: 配置 Value 的类型
    job.setMapOutputValueClass(classOf[KeyValue])

    val currentHFilePath = s"$hfilePath/$sourceTableName"
    hive2HFile(sourceDbName, sourceTableName, sourceKeyFieldName, currentHFilePath, hadoopConfig)
    hfile2HBase(job, sourceTableName, currentHFilePath)
  }

  /**
   * 通过 Spark 读取 Hive 表加载数据
   * 并且, 落地成为 HFile 格式
   *
   * 来源: Hive 表, sourceDbName.sourceTableName
   * 落地: HDFS 当中的 HFile
   */
  def hive2HFile(db: String, table: String, keyField: String, hfilePath: String, config: Configuration): Unit = {
    // 创建 SparkSession
    // 注意点: 不能指定 Master, spark-submit 提交的时候, 会指定 Master
    val spark = SparkSession.builder()
      .appName("extra data from hive to hfile")
      .enableHiveSupport()
      .getOrCreate()

    // 1. 加载数据
    // type DataFrame = Dataset[Row]
    val source: Dataset[Row] = spark.read.table(s"$db.$table")

    // 2. 处理数据, (ImmutableBytesWritable, KeyValue)
    val transfer: RDD[(ImmutableBytesWritable, KeyValue)] = source.rdd
      // 因为 map 是对一整行数据进行转换, 但是最终的出口应该多个单元格的数据 KV
      // 所以使用 flatMap, row -> convert -> multi cell KV
      .filter(row => row.getAs(keyField) != null)
      .sortBy(row => s"${row.getAs(keyField)}")
      .flatMap(row => {
        // HBase 中所有数据类型都是 Bytes
        // 取得 RowKey 的值
        val rowKeyBytes = Bytes.toBytes(s"${row.getAs(keyField)}")

        // 把 row 拆开, 转换为 cells 形式, 多个单元格, 对每个单元格处理, 转成 KV 形式出去
        val hbaseKV: Seq[(ImmutableBytesWritable, KeyValue)] = row.schema
          .filter(field => row.getAs(field.name) != null)
          .sortBy(field => field.name)
          .map(field => {
            // 取得列名, 将列名转为 KV 形式

            // 1. 获取 row 的每一个单元格的 field name 和 value
            val fieldNameBytes = Bytes.toBytes(field.name)
            val valueBytes = Bytes.toBytes(s"${row.getAs(field.name)}")

            // 2. 生成 KV 返回
            val kv = new KeyValue(rowKeyBytes, Bytes.toBytes(defaultHBaseCF), fieldNameBytes, valueBytes)

            (new ImmutableBytesWritable(rowKeyBytes), kv)
          })

        hbaseKV
      })

    // 3. 落地, HDFS -> HFile
    // HBase 提供了表的访问模式, 但是 HBase 底层存储的数据结构是 KV, K = rowkey:cf:c V=value
    // 所以, 我们要去将数据处理成 KV 的形式, 再落地成 HFile
    // 所以, 最终需要的数据格式是 (K, KV) => (RowKey, (RowKey, Value)) => (ImmutableBytesWritable, KeyValue)
    // (ImmutableBytesWritable, KeyValue)
    transfer.saveAsNewAPIHadoopFile(
      hfilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      config
    )
  }

  /**
   * 通过 HBase 的 API, 将 HFile 数据加载到 HBase 中
   */
  def hfile2HBase(job: Job, table: String, hfilePath: String): Unit = {
    // 1. 配置, connection, admin
    val connection = ConnectionFactory.createConnection(job.getConfiguration)
    val admin = connection.getAdmin

    // 2. 判断表是否存在
    val tableName = TableName.valueOf(Bytes.toBytes(hbaseDefaultNameSpace), Bytes.toBytes(table))

    if (!admin.tableExists(tableName)) {
      admin.createTable(
          TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder
              .newBuilder(Bytes.toBytes(defaultHBaseCF))
              .build()
            )
            .build()
        )
    }

    // 3. 调用 API 进行 BulkLoad
    val realTable = connection.getTable(tableName)
    val regionLocator = new HRegionLocator(tableName, connection.asInstanceOf[ClusterConnection])

    val loader = new LoadIncrementalHFiles(job.getConfiguration)
    loader.doBulkLoad(new Path(hfilePath), admin, realTable, regionLocator)
  }
}
