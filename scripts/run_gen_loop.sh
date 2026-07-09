#!/bin/bash
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 JAVA_HOME=/root/jdk1.8.0_171
cd /root/abyss-pipeline
CP=Gacha-Record-Generator/gacha-generator.jar:lib/jackson-core-2.15.0.jar:lib/jackson-databind-2.15.0.jar:lib/jackson-annotations-2.15.0.jar:jars/kafka-clients-2.1.0.jar:jars/slf4j-api-1.7.16.jar:jars/slf4j-log4j12-1.7.16.jar:jars/log4j-1.2.17.jar
CPB=Character-Build-Generator/build-generator.jar:lib/jackson-core-2.15.0.jar:lib/jackson-databind-2.15.0.jar:lib/jackson-annotations-2.15.0.jar:jars/kafka-clients-2.1.0.jar:jars/slf4j-api-1.7.16.jar:jars/slf4j-log4j12-1.7.16.jar:jars/log4j-1.2.17.jar
i=0
while true; do
  # 练度：每秒 10 条
  $JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -cp "$CPB" org.example.App build_stats.json --count 10 --kafka-bootstrap-servers Middleware:9092 --kafka-topic build-v2 2>&1 | grep Published
  # 抽卡：每秒 4 条
  $JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -cp "$CP" org.example.App pool_stats.json --version '6.6下半' --count 4 --kafka-bootstrap-servers Middleware:9092 --kafka-topic gacha-v2 2>&1 | grep Published
  i=$((i + 1))
  sleep 1
done
