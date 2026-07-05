import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 逐风数据洞察平台 — DWD层构建
 * ODS → DWD：清洗空值、统一版本名称格式、拆分配队
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class DataClean abyss-data-platform.jar
 */
object DataClean {

  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataClean-DWD")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("DWD层构建开始...")
    println("=" * 60)

    // ============ 1. 深渊使用率 → dwd_char_usage_timeline ============
    println("\n[1/3] 构建 dwd_char_usage_timeline (深渊数据)...")
    val abyssDF = spark.read.jdbc(JDBC_URL, "ods_abyss_usage", jdbcProps)

    val abyssClean = abyssDF
      .filter(col("char_name").isNotNull && col("char_name") =!= "")
      .filter(col("use_rate").isNotNull)
      .withColumn("data_source", lit("abyss"))
      .select(
        col("version_name"),
        col("char_name"),
        col("star"),
        col("use_count"),
        col("own_count"),
        col("use_rate"),
        col("own_rate"),
        col("tier"),
        col("data_source")
      )

    abyssClean.write
      .mode(SaveMode.Append)
      .jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)

    println(s"  ✓ 深渊数据已写入 dwd_char_usage_timeline")

    // ============ 2. 危战使用率 → 追加到 dwd_char_usage_timeline ============
    println("\n[2/3] 追加幽境危战数据到 dwd_char_usage_timeline...")
    val rank2DF = spark.read.jdbc(JDBC_URL, "ods_rank2_usage", jdbcProps)

    val rank2Clean = rank2DF
      .filter(col("char_name").isNotNull && col("char_name") =!= "")
      .filter(col("use_rate").isNotNull)
      .withColumn("data_source", lit("rank2"))
      .select(
        col("version_name"),
        col("char_name"),
        col("star"),
        col("use_count"),
        col("own_count"),
        col("use_rate"),
        col("own_rate"),
        col("tier"),
        col("data_source")
      )

    rank2Clean.write
      .mode(SaveMode.Append)
      .jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)

    println(s"  ✓ 危战数据已追加到 dwd_char_usage_timeline")

    // ============ 3. 配队拆分为角色明细 → dwd_team_detail ============
    println("\n[3/3] 拆分配队组合 → dwd_team_detail...")

    val teamDF = spark.read.jdbc(JDBC_URL, "ods_team_ranking", jdbcProps)

    // 将 "角色1 + 角色2 + 角色3 + 角色4" 拆分为4行
    // 使用 split + posexplode
    val teamDetail = teamDF
      .filter(col("team_comp").isNotNull && col("team_comp") =!= "")
      .withColumn("roles_array", split(col("team_comp"), "\\s*\\+\\s*"))
      .withColumn("exploded", posexplode(col("roles_array")))
      .withColumn("position", col("exploded._1") + 1)  // 位置1-4
      .withColumn("char_name", trim(col("exploded._2"))) // 角色名
      .filter(col("char_name") =!= "" && col("char_name") =!= "?")
      .select(
        col("version_name"),
        col("team_comp"),
        col("char_name"),
        col("position"),
        col("use_count"),
        col("use_rate"),
        col("attend_rate"),
        col("has_rate")
      )

    teamDetail.write
      .mode(SaveMode.Append)
      .jdbc(JDBC_URL, "dwd_team_detail", jdbcProps)

    val detailCount = teamDetail.count()
    println(s"  ✓ 配队明细已写入 dwd_team_detail ($detailCount 条)")

    println("\n" + "=" * 60)
    println("DWD层构建完成！")
    println("=" * 60)

    spark.stop()
  }
}
