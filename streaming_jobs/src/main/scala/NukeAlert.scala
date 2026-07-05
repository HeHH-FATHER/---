import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/**
 * 逐风数据洞察平台 — 核爆检测
 * Kafka battle_log → 过滤伤害>100万 → Redis List
 *
 * Redis Key:
 *   nuke:list  List  核爆记录TOP20
 *
 * 核爆事件JSON:
 *   {"uid":"51390***4","character":"胡桃","damage":1285630,"reaction":"蒸发","time":"2秒前"}
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar,jedis-2.9.0.jar \
 *   --class NukeAlert abyss-data-platform.jar
 */
object NukeAlert {

  val KAFKA_BOOTSTRAP = "Middleware:9092"
  val KAFKA_TOPIC     = "battle_log"
  val REDIS_HOST      = "Middleware"
  val REDIS_PORT      = 6379
  val NUKE_THRESHOLD  = 1000000L  // 100万伤害

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NukeAlert-Streaming")
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

    // 解析JSON，过滤核爆（伤害>100万）
    val parsedDF = kafkaDF
      .select(from_json(col("value").cast("string"), battleSchema).as("data"))
      .select("data.*")
      .filter(col("damage") >= NUKE_THRESHOLD)
      .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    // 写入Redis List
    val query = parsedDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[Row], batchId: Long) =>
        batchDF.collect().foreach { row =>
          var jedis: Jedis = null
          try {
            jedis = new Jedis(REDIS_HOST, REDIS_PORT)

            val uid     = Option(row.getString(5)).getOrElse("???")
            val uidMask = if (uid.length > 6) uid.take(5) + "***" + uid.takeRight(1) else uid
            val character = Option(row.getString(0)).getOrElse("???")
            val damage    = row.getLong(3)
            val reaction  = Option(row.getString(2)).getOrElse("")
            val ts        = row.getLong(4)

            val eventJson = s"""{"uid":"$uidMask","character":"$character","damage":$damage,"reaction":"$reaction","ts":$ts}"""

            jedis.lpush("nuke:list", eventJson)
            jedis.ltrim("nuke:list", 0, 19) // 只保留TOP20
            println(s"[NUKE] $character 打出核爆 $damage ($reaction)")
          } catch {
            case e: Exception => println(s"[ERROR] Redis写入失败: ${e.getMessage}")
          } finally {
            if (jedis != null) jedis.close()
          }
        }
      }
      .outputMode("append")
      .start()

    println("NukeAlert 启动完成，监听 Kafka battle_log...")
    println(s"核爆阈值: $NUKE_THRESHOLD (${NUKE_THRESHOLD / 10000}万)")
    query.awaitTermination()
  }
}
