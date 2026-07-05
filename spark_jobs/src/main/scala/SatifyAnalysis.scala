import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 逐风数据洞察平台 — 口碑-实力交叉分析
 * DWD + ODS → DWS: dws_satify_vs_usage
 *
 * 象限分类:
 * - use_rate > 30 AND avg_satify > 8 → "叫好叫座"
 * - use_rate <= 30 AND avg_satify > 8 → "叫好不叫座"
 * - use_rate > 30 AND avg_satify <= 8 → "叫座不叫好"
 * - use_rate <= 30 AND avg_satify <= 8 → "双低"
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class SatifyAnalysis abyss-data-platform.jar
 */
object SatifyAnalysis {

  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SatifyAnalysis-DWS")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("口碑-实力交叉分析开始...")
    println("=" * 60)

    // 获取最新一期深渊使用率
    val usageDF = spark.read.jdbc(JDBC_URL, "dwd_char_usage_timeline", jdbcProps)
      .filter(col("data_source") === "abyss")

    // 找最新版本：版本序号最大的
    val versionOrderDF = usageDF
      .select("version_name")
      .distinct()
      .withColumn("version_order", row_number().over(Window.orderBy(col("version_name").desc)))

    val latestVersion = versionOrderDF
      .filter(col("version_order") === 1)
      .select("version_name")
      .first()
      .getString(0)

    println(s"  最新深渊版本: $latestVersion")

    val latestUsageDF = usageDF
      .filter(col("version_name") === latestVersion)
      .select("char_name", "star", "use_rate", "own_rate")

    println(s"  最新版本角色数: ${latestUsageDF.count()}")

    // 读取满意度数据
    val voteDF = spark.read.jdbc(JDBC_URL, "ods_role_vote", jdbcProps)
      .select("char_name", "avg_ability", "avg_look", "avg_satify", "vote_sum")

    println(s"  满意度数据角色数: ${voteDF.count()}")

    // JOIN 使用率 + 满意度
    val joinedDF = latestUsageDF
      .join(voteDF, Seq("char_name"), "inner")
      .na.fill(0.0) // 填充空值

    // 象限分类
    val quadrantDF = joinedDF
      .withColumn("quadrant",
        when(col("use_rate") > 30 && col("avg_satify") > 8, "叫好叫座")
        .when(col("use_rate") <= 30 && col("avg_satify") > 8, "叫好不叫座")
        .when(col("use_rate") > 30 && col("avg_satify") <= 8, "叫座不叫好")
        .otherwise("双低")
      )
      .select(
        col("char_name"),
        col("star"),
        col("use_rate"),
        col("avg_ability"),
        col("avg_look"),
        col("avg_satify"),
        col("vote_sum"),
        col("quadrant")
      )

    // 展示各象限分布
    println("\n象限分布:")
    quadrantDF.groupBy("quadrant")
      .agg(count("*").as("count"))
      .orderBy(col("count").desc)
      .show()

    // 展示各象限代表角色
    println("叫好叫座 TOP10:")
    quadrantDF.filter(col("quadrant") === "叫好叫座")
      .orderBy(col("use_rate").desc)
      .select("char_name", "star", "use_rate", "avg_satify")
      .show(10, truncate = false)

    println("叫好不叫座 TOP10:")
    quadrantDF.filter(col("quadrant") === "叫好不叫座")
      .orderBy(col("avg_satify").desc)
      .select("char_name", "star", "use_rate", "avg_satify")
      .show(10, truncate = false)

    // 写入MySQL
    quadrantDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(JDBC_URL, "dws_satify_vs_usage", jdbcProps)

    println(s"\n  ✓ dws_satify_vs_usage 写入完成 (${quadrantDF.count()} 条)")

    println("\n" + "=" * 60)
    println("口碑-实力交叉分析完成！")
    println("=" * 60)

    spark.stop()
  }
}
