name := "abyss-data-platform"

version := "1.0"

scalaVersion := "2.11.12"

// Spark 2.4.0
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.47",
  "redis.clients" % "jedis" % "2.9.0"
)

// 合并器策略（避免与 provided 依赖冲突）
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
