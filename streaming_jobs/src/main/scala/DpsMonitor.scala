import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/**
 * 逐风数据洞察平台 — 全服DPS实时监控
 * Kafka battle_log → 5秒滑动窗口聚合 → Redis
 *
 * Redis Key:
 *   dps:total   String  当前5秒总DPS
 *   dps:history List   最近60秒DPS（12个点）
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar,jedis-2.9.0.jar \
 *   --class DpsMonitor abyss-data-platform.jar
 */
object DpsMonitor {

  val KAFKA_BOOTSTRAP = "Middleware:9092"
  val KAFKA_TOPIC     = "battle_log"
  val REDIS_HOST      = "Middleware"
  val REDIS_PORT      = 6379

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DpsMonitor-Streaming")
      .getOrCreate()

    import spark.implicits._

    // 读取Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    // 定义JSON Schema
    val battleSchema = StructType(Array(
      StructField("character", StringType),
      StructField("skill_type", StringType),
      StructField("reaction", StringType),
      StructField("damage", LongType),
      StructField("timestamp", LongType),
      StructField("uid", StringType)
    ))

    // 解析JSON
    val parsedDF = kafkaDF
      .select(from_json(col("value").cast("string"), battleSchema).as("data"))
      .select("data.*")
      .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    // 5秒滑动窗口聚合总DPS
    val windowedDPS = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "5 seconds", "5 seconds"))
      .agg(sum("damage").as("total_dps"))
      .select(col("window.start").as("window_start"), col("total_dps"))

    // 写入Redis
    val query = windowedDPS.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        batchDF.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            // Redis无密码
            while (rows.hasNext) {
              val row = rows.next()
              val dps = row.getLong(1)
              if (dps > 0) {
                jedis.set("dps:total", dps.toString)
                jedis.lpush("dps:history", dps.toString)
                jedis.ltrim("dps:history", 0, 11) // 只保留最近12个点
                println(s"[DPS] batch=$batchId dps=$dps")
              }
            }
          } catch {
            case e: Exception => println(s"[ERROR] Redis写入失败: ${e.getMessage}")
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .outputMode("update")
      .start()

    println("DpsMonitor 启动完成，监听 Kafka battle_log...")
    query.awaitTermination()
  }
}
