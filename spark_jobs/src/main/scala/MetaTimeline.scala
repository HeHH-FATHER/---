import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 逐风数据洞察平台 — 版本Meta演化分析
 * DWD → DWS: dws_char_usage_avg + dws_char_usage_trend
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class MetaTimeline abyss-data-platform.jar
 */
object MetaTimeline {

  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MetaTimeline-DWS")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("Meta演化分析开始...")
    println("=" * 60)

    // 读取深渊使用率数据
    val usageDF = spark.read.jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)
      .filter(col("data_source") === "abyss")

    // ============ 1. 角色跨版本平均使用率 → dws_char_usage_avg ============
    println("\n[1/2] 计算 dws_char_usage_avg...")

    val charAvgDF = usageDF
      .groupBy("char_name")
      .agg(
        max("star").as("star"),
        avg("use_rate").as("avg_use_rate"),
        max("use_rate").as("max_use_rate"),
        min("use_rate").as("min_use_rate"),
        count("version_name").as("version_count")
      )
      .withColumn("avg_use_rate", round(col("avg_use_rate"), 2))
      .withColumn("max_use_rate", round(col("max_use_rate"), 2))
      .withColumn("min_use_rate", round(col("min_use_rate"), 2))

    // 计算趋势方向：使用"版本序号"（按version_name排序）代替日期
    val versionOrderDF = usageDF
      .select("version_name")
      .distinct()
      .withColumn("version_order", row_number().over(Window.orderBy("version_name")))

    val usageWithOrder = usageDF
      .join(versionOrderDF, "version_name")

    // 每个角色最近3期的使用率趋势
    val charVersionWindow = Window.partitionBy("char_name").orderBy(col("version_order").desc)
    val recent3DF = usageWithOrder
      .withColumn("rn", row_number().over(charVersionWindow))
      .filter(col("rn") <= 3)
      .groupBy("char_name")
      .agg(
        collect_list(struct(col("version_order"), col("use_rate"))).as("recent_data")
      )

    // 判断趋势：最近3期持续上升→"上升"，持续下降→"下降"，否则"稳定"
    val trendUDF = udf { (data: Seq[org.apache.spark.sql.Row]) =>
      if (data == null || data.size < 2) "稳定"
      else {
        val rates = data.sortBy(_.getInt(0)).map(_.getAs[BigDecimal](1).doubleValue())
        if (rates.length >= 2) {
          val inc = rates.sliding(2).forall(p => p(1) > p(0))
          val dec = rates.sliding(2).forall(p => p(1) < p(0))
          if (inc) "上升" else if (dec) "下降" else "稳定"
        } else "稳定"
      }
    }

    val resultAvgDF = charAvgDF
      .join(recent3DF, Seq("char_name"), "left")
      .withColumn("trend_direction", trendUDF(col("recent_data")))
      .select("char_name", "star", "avg_use_rate", "max_use_rate", "min_use_rate",
              "version_count", "trend_direction")

    resultAvgDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(JDBC_URL, "dws_char_usage_avg", jdbcProps)

    println(s"  ✓ dws_char_usage_avg 写入完成 (${resultAvgDF.count()} 条)")

    // ============ 2. 逐期变化趋势 → dws_char_usage_trend ============
    println("\n[2/2] 计算 dws_char_usage_trend (LAG窗口函数)...")

    val charWindow = Window.partitionBy("char_name").orderBy("version_order")

    val trendDF = usageWithOrder
      .withColumn("prev_use_rate", lag("use_rate", 1).over(charWindow))
      .withColumn("change_pct",
        when(col("prev_use_rate").isNotNull,
          round(col("use_rate") - col("prev_use_rate"), 2))
        .otherwise(null))
      .withColumn("rank_current",
        rank().over(Window.partitionBy("version_name").orderBy(col("use_rate").desc)))
      .withColumn("rank_prev",
        when(col("prev_use_rate").isNotNull,
          lag(col("rank_current"), 1).over(charWindow))
        .otherwise(null))
      .select(
        col("char_name"),
        col("version_name"),
        col("use_rate"),
        col("prev_use_rate"),
        col("change_pct"),
        col("rank_current"),
        col("rank_prev")
      )
      .filter(col("prev_use_rate").isNotNull)

    trendDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(JDBC_URL, "dws_char_usage_trend", jdbcProps)

    println(s"  ✓ dws_char_usage_trend 写入完成 (${trendDF.count()} 条)")

    println("\n" + "=" * 60)
    println("Meta演化分析完成！")
    println("=" * 60)

    spark.stop()
  }
}
