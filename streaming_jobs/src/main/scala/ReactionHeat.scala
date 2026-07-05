import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/**
 * 逐风数据洞察平台 — 元素反应热力监控
 * Kafka battle_log → 反应计数 → Redis Hash
 *
 * Redis Key:
 *   reaction:heat  Hash  {蒸发:350, 激化:220, 超载:140, ...}
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar,jedis-2.9.0.jar \
 *   --class ReactionHeat abyss-data-platform.jar
 */
object ReactionHeat {

  val KAFKA_BOOTSTRAP = "Middleware:9092"
  val KAFKA_TOPIC     = "battle_log"
  val REDIS_HOST      = "Middleware"
  val REDIS_PORT      = 6379

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReactionHeat-Streaming")
      .getOrCreate()

    import spark.implicits._

    // 读取Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    // 定义JSON Schema（只解析reaction字段）
    val battleSchema = StructType(Array(
      StructField("character", StringType),
      StructField("skill_type", StringType),
      StructField("reaction", StringType),
      StructField("damage", LongType),
      StructField("timestamp", LongType),
      StructField("uid", StringType)
    ))

    // 解析JSON，过滤有效反应
    val parsedDF = kafkaDF
      .select(from_json(col("value").cast("string"), battleSchema).as("data"))
      .select("data.*")
      .filter(col("reaction").isNotNull && col("reaction") =!= "")
      .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    // 5秒窗口内各反应计数
    val reactionCounts = parsedDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(
        window(col("event_time"), "5 seconds", "5 seconds"),
        col("reaction")
      )
      .agg(count("*").as("cnt"))

    // 写入Redis Hash
    val query = reactionCounts.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        batchDF.collect().foreach { row =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)
            val reaction = row.getString(1)
            val cnt = row.getLong(2)
            jedis.hincrBy("reaction:heat", reaction, cnt)
            jedis.expire("reaction:heat", 60) // 60秒过期
            println(s"[Reaction] $reaction +$cnt")
          } catch {
            case e: Exception => println(s"[ERROR] Redis写入失败: ${e.getMessage}")
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .outputMode("update")
      .start()

    println("ReactionHeat 启动完成，监听 Kafka battle_log...")
    query.awaitTermination()
  }
}
