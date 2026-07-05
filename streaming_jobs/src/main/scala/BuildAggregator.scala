import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/**
 * 逐风数据洞察平台 — 实时链② 练度聚合
 * Kafka build_log → 5秒滑动窗口聚合 → Redis
 *
 * Redis Key:
 *   build:avg_const   String  平均命座
 *   build:avg_damage  String  平均伤害
 *   build:top_weapon  String  使用率最高武器
 *   build:top_arti    String  使用率最高圣遗物
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars jedis-2.9.0.jar,spark-sql-kafka-0-10_2.11-2.4.0.jar \
 *   --class BuildAggregator abyss-streaming-platform.jar
 */
object BuildAggregator {

  val KAFKA_BOOTSTRAP = "Middleware:9092"
  val KAFKA_TOPIC     = "build_log"
  val REDIS_HOST      = "Middleware"
  val REDIS_PORT      = 6379

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BuildAggregator-Streaming")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    val buildSchema = StructType(Array(
      StructField("uid", StringType),
      StructField("char", StringType),
      StructField("star", IntegerType),
      StructField("constellation", IntegerType),
      StructField("level", IntegerType),
      StructField("weapon", StringType),
      StructField("artifact", StringType),
      StructField("damage", LongType),
      StructField("timestamp", LongType)
    ))

    val parsedDF = kafkaDF
      .select(from_json(col("value").cast("string"), buildSchema).as("data"))
      .select("data.*")
      .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    // 5秒窗口聚合：平均命座 + 平均伤害
    val windowed = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "5 seconds", "5 seconds"))
      .agg(
        avg("constellation").as("avg_const"),
        avg("damage").as("avg_damage")
      )
      .select(
        col("window.start").as("window_start"),
        col("avg_const"),
        col("avg_damage")
      )

    val query = windowed.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        batchDF.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            while (rows.hasNext) {
              val row = rows.next()
              val avgC = f"${row.getDouble(1)}%.2f"
              val avgD = row.getLong(2)
              jedis.set("build:avg_const", avgC)
              jedis.set("build:avg_damage", avgD.toString)
              println(s"[Build] batch=$batchId avgConst=$avgC avgDmg=$avgD")
            }
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .start()

    // TOP武器聚合（10秒窗口）
    val weaponQuery = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "10 seconds", "10 seconds"), col("weapon"))
      .count()
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        val top = batchDF.orderBy(col("count").desc).select("weapon").limit(1)
        top.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            while (rows.hasNext) {
              val w = rows.next().getString(0)
              if (w != null && w.nonEmpty) jedis.set("build:top_weapon", w)
            }
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .start()

    // TOP圣遗物聚合（10秒窗口）
    val artiQuery = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(window(col("event_time"), "10 seconds", "10 seconds"), col("artifact"))
      .count()
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        val top = batchDF.orderBy(col("count").desc).select("artifact").limit(1)
        top.foreachPartition { (rows: java.util.Iterator[Row]) =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            while (rows.hasNext) {
              val a = rows.next().getString(0)
              if (a != null && a.nonEmpty) jedis.set("build:top_arti", a)
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
