#!/bin/bash
# 逐风数据洞察平台 — Spark Streaming 消费端启动脚本
# 替换 Python 消费端：realtime_consumer.py + satisfaction_consumer.py + hot_char_aggregator.py
# 运行位置：master0 → Spark standalone cluster（各 1 Core）

set -e
SPARK_HOME=/root/spark-2.4.0
JAR=/root/abyss-pipeline/Character-Build-Generator/build-generator.jar
JARS_DIR=/root/abyss-pipeline/jars
JARS=$JARS_DIR/jedis-2.9.0.jar,$JARS_DIR/kafka-clients-2.1.0.jar,$JARS_DIR/spark-streaming-kafka-0-10_2.11-2.4.0.jar,$JARS_DIR/commons-pool2-2.4.2.jar
KAFKA_BIN="/root/kafka2/bin"
export LANG=en_US.UTF-8

echo "=== 停止旧 Python 消费端 (Middleware) ==="
ssh Middleware "pkill -f realtime_consumer 2>/dev/null; pkill -f hot_char_aggregator 2>/dev/null; pkill -f satisfaction_consumer 2>/dev/null" || true
sleep 1

echo "=== 停止旧 Spark Streaming ==="
pkill -f CharacterBuildStreamingConsumer 2>/dev/null || true
pkill -f SatisfactionStreamingConsumer 2>/dev/null || true
sleep 2

echo "=== 清理 Spark checkpoint（避免回放历史数据） ==="
hdfs dfs -rm -r -skipTrash /tmp/spark-build-v2-checkpoint 2>/dev/null || true
hdfs dfs -rm -r -skipTrash /tmp/spark-sat-v1-checkpoint 2>/dev/null || true

echo "=== 重置 Kafka 消费偏移到最新（关键！避免重启后重放全量历史） ==="
echo "  (跳过活跃 group 的 offset reset)"
ssh Middleware "$KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server Middleware:9092 --group spark-satisfaction-v2 --reset-offsets --to-latest --all-topics --execute 2>/dev/null" || true
ssh Middleware "$KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server Middleware:9092 --group spark-build-v2 --reset-offsets --to-latest --all-topics --execute 2>/dev/null" || true

echo "=== 启动 Build Streaming (build-v2, batch=1s, 集群模式) ==="
nohup $SPARK_HOME/bin/spark-submit \
  --class org.example.streaming.CharacterBuildStreamingConsumer \
  --master spark://master0:7077 --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --jars $JARS \
  $JAR Middleware:9092 build-v2 Middleware 6379 1 \
  > /tmp/spark_build.log 2>&1 &
echo "  Build PID: $!"

echo "=== 启动 Satisfaction Streaming (satisfaction-v1, batch=3s, 集群模式) ==="
nohup $SPARK_HOME/bin/spark-submit \
  --class org.example.streaming.SatisfactionStreamingConsumer \
  --master spark://master0:7077 --driver-memory 512m --executor-memory 512m \
  --conf spark.cores.max=1 --jars $JARS \
  $JAR Middleware:9092 satisfaction-v1 Middleware 6379 3 \
  > /tmp/spark_sat.log 2>&1 &
echo "  Sat PID: $!"

sleep 10
echo ""
echo "=== 验证 ==="
echo "Spark 进程: $(ps aux | grep SparkSubmit | grep -v grep | wc -l)（期望 2）"
echo "Spark Workers: curl -s http://localhost:8080/json/ | python3 -c 'import json,sys; d=json.load(sys.stdin); [print(w[\"host\"],\"cores\",w[\"coresused\"]) for w in d[\"workers\"]]'"
echo ""
echo "Redis 验证:"
echo "  redis-cli -h Middleware LLEN build:recent       # 期望 20"
echo "  redis-cli -h Middleware ZCARD rt:satisfaction:ranking  # 期望 121"
echo ""
echo "日志:"
echo "  tail -f /tmp/spark_build.log"
echo "  tail -f /tmp/spark_sat.log"
echo ""
echo "Spark UI: http://master0:8080  (集群总览，应看到 2 个 Running Applications)"
echo ""
echo "=== 启动链路监控采集 ==="
ssh Middleware "cd /root/abyss-pipeline && pkill -f monitor_collector 2>/dev/null; nohup python3 -u scripts/monitor_collector.py --loop > /tmp/monitor.log 2>&1 & disown" 2>/dev/null || true
cd /root/abyss-pipeline && pkill -f monitor_master0 2>/dev/null; nohup python3 -u scripts/monitor_master0.py --loop > /tmp/monitor_m0.log 2>&1 & disown
sleep 2
echo "  监控: $(ps aux | grep 'monitor_master0\|monitor_collector' | grep -v grep | wc -l) 进程（期望 1，MW 侧通过 SSH 启动）"
