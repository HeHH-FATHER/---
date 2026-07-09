#!/bin/bash
cd /root/abyss-pipeline
JAVA=/root/jdk1.8.0_171/bin/java
CP=lib/jackson-core-2.15.0.jar:lib/jackson-databind-2.15.0.jar:lib/jackson-annotations-2.15.0.jar:jars/kafka-clients-2.1.0.jar:jars/slf4j-api-1.7.16.jar:jars/slf4j-log4j12-1.7.16.jar:jars/log4j-1.2.17.jar
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

pkill -f org.example.App 2>/dev/null
sleep 1

$JAVA -Dfile.encoding=UTF-8 -cp Character-Build-Generator/build-generator.jar:$CP org.example.App build_stats.json --count 10 --kafka-bootstrap-servers Middleware:9092 --kafka-topic build-v2 --loop > /tmp/build_gen.log 2>&1 &
disown

$JAVA -Dfile.encoding=UTF-8 -cp Character-Build-Generator/build-generator.jar:$CP org.example.satisfaction.SatisfactionProducerApp 提瓦特数据/角色满意度排行.json --kafka-bootstrap-servers Middleware:9092 --kafka-topic satisfaction-v1 --loop --interval 3 > /tmp/satisfaction.log 2>&1 &
disown

sleep 2
echo "Build: $(ps aux | grep 'build_stats' | grep -v grep | wc -l)"
echo "Satisfaction: $(ps aux | grep 'Satisfaction' | grep -v grep | wc -l)"

# 同时启动离线管道（抽卡，每5分钟5000抽→MySQL）
echo ""
echo "=== 启动离线管道 ==="
bash scripts/start_offline_pipelines.sh
