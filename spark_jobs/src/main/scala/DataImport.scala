import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

/**
 * 逐风数据洞察平台 — 数据导入作业
 * 从HDFS读取CSV → 写入MySQL ODS表
 *
 * 运行方式:
 * spark-submit --master yarn --deploy-mode client \
 *   --driver-memory 1g --executor-memory 2g \
 *   --jars mysql-connector-java-5.1.47.jar \
 *   --class DataImport abyss-data-platform.jar
 */
object DataImport {

  // ==================== 配置 ====================
  val JDBC_URL  = "jdbc:mysql://100.103.177.85:3306/abyss_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
  val JDBC_USER = "root"
  val JDBC_PASS = "123456"
  val HDFS_URI  = "hdfs://master0:9000"

  val jdbcProps = new java.util.Properties()
  jdbcProps.setProperty("user", JDBC_USER)
  jdbcProps.setProperty("password", JDBC_PASS)
  jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataImport-ODS")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("数据导入作业开始...")
    println("=" * 60)

    // ---- 1. abyss_usage → ods_abyss_usage ----
    println("\n[1/5] 导入深渊使用率...")
    loadAndInsert(spark, s"$HDFS_URI/data/abyss/abyss_usage.csv", "ods_abyss_usage",
      schema = StructType(Array(
        StructField("version_name", StringType),
        StructField("char_name", StringType),
        StructField("star", IntegerType),
        StructField("use_count", IntegerType),
        StructField("own_count", IntegerType),
        StructField("use_rate", DecimalType(5,2)),
        StructField("own_rate", DecimalType(5,2)),
        StructField("tier", StringType)
      ))
    )

    // ---- 2. rank2_usage → ods_rank2_usage ----
    println("\n[2/5] 导入幽境危战使用率...")
    loadAndInsert(spark, s"$HDFS_URI/data/rank2/rank2_usage.csv", "ods_rank2_usage",
      schema = StructType(Array(
        StructField("version_name", StringType),
        StructField("char_name", StringType),
        StructField("star", IntegerType),
        StructField("use_count", IntegerType),
        StructField("own_count", IntegerType),
        StructField("use_rate", DecimalType(5,2)),
        StructField("own_rate", DecimalType(5,2)),
        StructField("tier", StringType)
      ))
    )

    // ---- 3. team_ranking → ods_team_ranking ----
    println("\n[3/5] 导入配队排行...")
    loadAndInsert(spark, s"$HDFS_URI/data/team/team_ranking.csv", "ods_team_ranking",
      schema = StructType(Array(
        StructField("version_name", StringType),
        StructField("team_comp", StringType),
        StructField("use_count", IntegerType),
        StructField("use_rate", DecimalType(5,2)),
        StructField("attend_rate", DecimalType(5,2)),
        StructField("has_rate", DecimalType(5,2)),
        StructField("up_use", IntegerType),
        StructField("down_use", IntegerType)
      ))
    )

    // ---- 4. role_avg → ods_role_avg ----
    println("\n[4/5] 导入角色练度...")
    loadAndInsert(spark, s"$HDFS_URI/data/role/role_avg.csv", "ods_role_avg",
      schema = StructType(Array(
        StructField("char_name", StringType),
        StructField("star", IntegerType),
        StructField("player_count", LongType),
        StructField("avg_level", DecimalType(4,1)),
        StructField("avg_constellation", DecimalType(3,2)),
        StructField("avg_damage", LongType),
        StructField("damage_type", StringType),
        StructField("top_weapon", StringType),
        StructField("top_artifact", StringType)
      ))
    )

    // ---- 5. role_vote → ods_role_vote ----
    println("\n[5/5] 导入角色满意度...")
    loadAndInsert(spark, s"$HDFS_URI/data/role/role_vote.csv", "ods_role_vote",
      schema = StructType(Array(
        StructField("char_name", StringType),
        StructField("star", IntegerType),
        StructField("avg_ability", DecimalType(3,1)),
        StructField("avg_look", DecimalType(3,1)),
        StructField("avg_satify", DecimalType(3,1)),
        StructField("vote_sum", IntegerType),
        StructField("favorite", IntegerType)
      ))
    )

    println("\n" + "=" * 60)
    println("数据导入作业完成！")
    println("=" * 60)

    spark.stop()
  }

  /**
   * 从HDFS读CSV并写入MySQL
   */
  def loadAndInsert(spark: SparkSession, hdfsPath: String, tableName: String,
                    schema: StructType): Unit = {
    try {
      val df = spark.read
        .option("header", "true")
        .option("charset", "UTF-8")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(hdfsPath)

      val count = df.count()
      println(s"  读取 $hdfsPath → $count 条记录")

      df.write
        .mode(SaveMode.Append)
        .jdbc(JDBC_URL, tableName, jdbcProps)

      println(s"  ✓ 写入 $tableName 成功 ($count 条)")
    } catch {
      case e: Exception =>
        println(s"  ✗ $tableName 导入失败: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
