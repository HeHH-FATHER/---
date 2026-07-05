import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/**
 * 逐风数据洞察平台 — 实时链① 抽卡聚合
 * Kafka gacha_log → 5秒滑动窗口聚合 → Redis
 *
 * Redis Key:
 *   gacha:pull_count  String  窗口抽取次数
 *   gacha:five_star   String  五星出货率(%)
 *   gacha:top_char    String  最热角色名
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars jedis-2.9.0.jar,spark-sql-kafka-0-10_2.11-2.4.0.jar \
 *   --class GachaAggregator abyss-streaming-platform.jar
 */
object GachaAggregator {

  val KAFKA_BOOTSTRAP = "Middleware:9092"
  val KAFKA_TOPIC     = "gacha_log"
  val REDIS_HOST      = "Middleware"
  val REDIS_PORT      = 6379

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GachaAggregator-Streaming")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    val gachaSchema = StructType(Array(
      StructField("uid", StringType),
      StructField("banner", StringType),
      StructField("item", StringType),
      StructField("star", IntegerType),
      StructField("timestamp", LongType),
      StructField("pity_count", IntegerType)
    ))

    val parsedDF = kafkaDF
      .select(from_json(col("value").cast("string"), gachaSchema).as("data"))
      .select("data.*")
      .withColumn("event_time", (col("timestamp")).cast("timestamp"))

    // 5秒窗口聚合
    val windowed = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "5 seconds", "5 seconds"))
      .agg(
        count("*").as("pull_count"),
        sum(when(col("star") === 5, 1).otherwise(0)).as("five_count")
      )
      .select(
        col("window.start").as("window_start"),
        col("pull_count"),
        col("five_count"),
        (col("five_count").cast("double") / col("pull_count") * 100).as("five_rate")
      )

    // 写入Redis
    val query = windowed.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        batchDF.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            while (rows.hasNext) {
              val row = rows.next()
              val count = row.getLong(1)
              val rate  = f"${row.getDouble(3)}%.1f"
              if (count > 0) {
                jedis.set("gacha:pull_count", count.toString)
                jedis.set("gacha:five_star", rate)
                println(s"[Gacha] batch=$batchId pulls=$count fiveRate=$rate%")
              }
            }
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .start()

    // 同时聚合每个窗口的TOP角色（单独查询Redis写入）
    val topCharQuery = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "10 seconds", "10 seconds"), col("item"))
      .count()
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        val top = batchDF.orderBy(col("count").desc).select("item").limit(1)
        top.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            while (rows.hasNext) {
              val c = rows.next().getString(0)
              if (c != null && c.nonEmpty) {
                jedis.set("gacha:top_char", c)
              }
            }
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .start()

    spark.streams.awaitAnyTermination()
  }
}
