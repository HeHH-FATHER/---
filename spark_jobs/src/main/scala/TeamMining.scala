import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.fpm.FPGrowth

/**
 * 逐风数据洞察平台 — 配队频繁项挖掘
 * DWD → DWS: dws_team_freq_items
 *
 * 使用FP-Growth算法挖掘角色共现模式：
 * - 每笔交易 = 一支队伍（4个角色）
 * - minSupport = 0.01（至少1%的队伍中同时出现）
 * - minConfidence = 0.1
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 2g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class TeamMining abyss-data-platform.jar
 */
object TeamMining {

  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  case class FreqResult(char_pair: String, cooccur_count: Int,
                        support: Double, confidence: Double, lift: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TeamMining-DWS")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("配队频繁项挖掘开始...")
    println("=" * 60)

    // 读取配队明细
    val teamDF = spark.read.jdbc(JDBC_URL, "dwd_team_detail", jdbcProps)

    // 将每支队伍的角色收集为数组 → 这就是FP-Growth的"交易"
    // group by (version_name, team_comp) → collect_list(char_name)
    val transactions = teamDF
      .groupBy("version_name", "team_comp")
      .agg(collect_list("char_name").as("items"))
      .filter(size(col("items")) >= 2)  // 至少2个角色
      .select("items")

    val txCount = transactions.count()
    println(s"  共 $txCount 笔交易（队伍）")

    // ===== FP-Growth =====
    val fpg = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.01)
      .setMinConfidence(0.1)

    val model = fpg.fit(transactions)

    // 频繁项集（2-项集 = 角色对）
    println("\n频繁2-项集（角色对）Top 20:")
    model.freqItemsets
      .filter(size(col("items")) === 2)
      .orderBy(col("freq").desc)
      .show(20, truncate = false)

    // 关联规则 → 写入MySQL
    println("\n关联规则（角色A → 角色B）:")
    val rules = model.associationRules
      .withColumn("antecedent_str", array_join(col("antecedent"), "+"))
      .withColumn("consequent_str", array_join(col("consequent"), "+"))
      .withColumn("char_pair", concat(col("antecedent_str"), lit("-"), col("consequent_str")))
      .filter(col("lift") > 1.0)  // lift > 1 才算有效关联
      .orderBy(col("lift").desc)

    val resultDF = rules.select(
      col("char_pair"),
      col("freq").as("cooccur_count").cast("int"),
      round(col("support"), 4).as("support"),
      round(col("confidence"), 4).as("confidence"),
      round(col("lift"), 2).as("lift")
    )

    resultDF.show(15, truncate = false)

    resultDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(JDBC_URL, "dws_team_freq_items", jdbcProps)

    println(s"\n  ✓ dws_team_freq_items 写入完成 (${resultDF.count()} 条)")

    // ===== 同时生成双向角色对（A+B和B+A都算共现）=====
    println("\n生成双向共现对...")
    val allPairs = teamDF
      .select("version_name", "team_comp", "char_name")
      .groupBy("version_name", "team_comp")
      .agg(collect_list("char_name").as("chars"))
      .as[(String, String, Seq[String])]
      .flatMap { case (_, _, chars) =>
        val sorted = chars.distinct.filter(_ != "?")
        for {
          i <- sorted.indices
          j <- (i + 1) until sorted.length
        } yield (sorted(i), sorted(j))
      }
      .toDF("char_a", "char_b")

    val pairCounts = allPairs
      .groupBy("char_a", "char_b")
      .agg(count("*").as("weight"))
      .orderBy(col("weight").desc)
      .limit(30)

    pairCounts.show(30, truncate = false)

    println("\n" + "=" * 60)
    println("配队频繁项挖掘完成！")
    println("=" * 60)

    spark.stop()
  }
}
