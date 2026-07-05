import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 逐风数据洞察平台 — ADS大屏宽表构建
 * DWS → ADS: 5张大屏消费表
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class AdsBuilder abyss-data-platform.jar
 */
object AdsBuilder {

  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdsBuilder-ADS")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("ADS大屏宽表构建开始...")
    println("=" * 60)

    // ============ 1. ads_meta_ranking: 使用率红黑榜 ============
    println("\n[1/5] 构建 ads_meta_ranking...")

    // 取最新版本深渊数据
    val usageDF = spark.read.jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)
      .filter(col("data_source") === "abyss")

    val latestVersion = usageDF
      .groupBy().agg(max("version_name").as("latest")).first().getString(0)

    val latestUsage = usageDF.filter(col("version_name") === latestVersion)

    // 按使用率排名
    val rankedDF = latestUsage
      .withColumn("rank_num", row_number().over(Window.orderBy(col("use_rate").desc)))
      .cache()

    val totalChars = rankedDF.count()

    // 红榜 TOP10 + 黑榜 BOTTOM10
    val redDF = rankedDF
      .filter(col("rank_num") <= 10)
      .withColumn("list_type", lit("red"))
      .select(col("rank_num"), col("char_name"), col("star"), lit(null).as("avatar"),
              col("use_rate"), col("own_rate"), lit("→").as("trend"), col("list_type"))

    val blackDF = rankedDF
      .filter(col("rank_num") > totalChars - 10)
      .withColumn("list_type", lit("black"))
      .select(col("rank_num"), col("char_name"), col("star"), lit(null).as("avatar"),
              col("use_rate"), col("own_rate"), lit("→").as("trend"), col("list_type"))

    val metaRankingDF = redDF.union(blackDF)
    metaRankingDF.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, "ads_meta_ranking", jdbcProps)
    println(s"  ✓ ads_meta_ranking (${metaRankingDF.count()} 条)")

    // ============ 2. ads_char_trend: 角色长青榜 ============
    println("\n[2/5] 构建 ads_char_trend...")

    // 取TOP15使用率角色，聚合全版本使用率趋势
    val top15Chars = latestUsage
      .orderBy(col("use_rate").desc)
      .limit(15)
      .select("char_name", "star")
      .collect()
      .map(r => (r.getString(0), r.getInt(1)))

    // 为每个TOP15角色构建版本历史JSON
    val topNames = top15Chars.map(_._1).toSeq
    val trendUsageDF = usageDF
      .filter(col("char_name").isin(topNames: _*))

    // 按角色聚合版本列表和使用率列表
    val charTrendDF = trendUsageDF
      .groupBy("char_name")
      .agg(
        first("star").as("star"),
        sort_array(collect_list(struct(col("version_name"), col("use_rate")))).as("history")
      )
      .withColumn("version_list",
        concat(lit("["), concat_ws(",", col("history.version_name")), lit("]")))
      .withColumn("rate_list",
        concat(lit("["), concat_ws(",", col("history.use_rate")), lit("]")))
      .select(col("char_name"), col("star"), lit(null).as("avatar"),
              col("version_list"), col("rate_list"))

    charTrendDF.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, "ads_char_trend", jdbcProps)
    println(s"  ✓ ads_char_trend (${charTrendDF.count()} 条)")

    // ============ 3. ads_satify_scatter: 口碑-实力散点图 ============
    println("\n[3/5] 构建 ads_satify_scatter...")

    val satifyDF = spark.read.jdbc(JDBC_URL, "dws_satify_vs_usage", jdbcProps)

    val scatterDF = satifyDF
      .withColumn("bubble_size",
        when(col("vote_sum").isNull, 10)
        .otherwise((log(col("vote_sum") + 1) * 5).cast("int")))
      .select(col("char_name"), col("star"), lit(null).as("avatar"),
              col("avg_ability").as("ability_score"),
              col("use_rate"),
              col("avg_satify").as("satify_score"),
              col("vote_sum").as("vote_count"),
              col("bubble_size"))

    scatterDF.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, "ads_satify_scatter", jdbcProps)
    println(s"  ✓ ads_satify_scatter (${scatterDF.count()} 条)")

    // ============ 4. ads_team_network: 配队共现网络 ============
    println("\n[4/5] 构建 ads_team_network...")

    val teamDetailDF = spark.read.jdbc(JDBC_URL, "dwd_team_detail", jdbcProps)

    // 计算角色共现次数
    val cooccurDF = teamDetailDF
      .select("version_name", "team_comp", "char_name")
      .groupBy("version_name", "team_comp")
      .agg(collect_list("char_name").as("chars"))
      .as[org.apache.spark.sql.Row]
      .flatMap { row =>
        val chars = row.getSeq[String](2).distinct.filter(_ != "?")
        for {
          i <- chars.indices
          j <- (i + 1) until chars.length
        } yield (chars(i), chars(j))
      }
      .toDF("source", "target")
      .groupBy("source", "target")
      .agg(count("*").as("weight"))
      .filter(col("weight") >= 2)  // 至少共现2次

    // 添加头像URL
    val roleListDF = spark.read.jdbc(JDBC_URL, "ods_role_list", jdbcProps)
      .select(col("char_name").as("rn"), col("avatar").as("av"))

    val networkDF = cooccurDF
      .join(roleListDF, cooccurDF("source") === roleListDF("rn"), "left")
      .withColumn("source_avatar", col("av"))
      .drop("rn", "av")
      .join(roleListDF, cooccurDF("target") === roleListDF("rn"), "left")
      .withColumn("target_avatar", col("av"))
      .drop("rn", "av")
      .orderBy(col("weight").desc)
      .limit(30)
      .select(col("source").as("source_name"), col("target").as("target_name"),
              col("source_avatar"), col("target_avatar"), col("weight"))

    networkDF.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, "ads_team_network", jdbcProps)
    println(s"  ✓ ads_team_network (${networkDF.count()} 条)")

    // ============ 5. ads_rank2_ranking: 幽境危战排行 ============
    println("\n[5/5] 构建 ads_rank2_ranking...")

    val rank2LatestDF = spark.read.jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)
      .filter(col("data_source") === "rank2")
      .groupBy()
      .agg(max("version_name").as("latest"))
      .first()
      .getString(0)

    val rank2RankingDF = spark.read.jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)
      .filter(col("data_source") === "rank2" && col("version_name") === rank2LatestDF)
      .withColumn("rank_num", row_number().over(Window.orderBy(col("use_rate").desc)))
      .filter(col("rank_num") <= 10)
      .select(col("rank_num"), col("char_name"), col("star"),
              lit(null).as("avatar"), col("use_rate"))

    rank2RankingDF.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, "ads_rank2_ranking", jdbcProps)
    println(s"  ✓ ads_rank2_ranking (${rank2RankingDF.count()} 条)")

    rankedDF.unpersist()

    println("\n" + "=" * 60)
    println("ADS大屏宽表构建完成！")
    println("=" * 60)

    spark.stop()
  }
}
