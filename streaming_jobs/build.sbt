name := "abyss-streaming-platform"

version := "1.0"

scalaVersion := "2.11.12"

// ========== Spark 2.4.0 + Kafka + Redis ==========
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"        % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql"         % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming"   % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  "redis.clients"    %  "jedis"             % "2.9.0"
)

// ========== 合并策略 ==========
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// ========== JAR 名称 ==========
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
