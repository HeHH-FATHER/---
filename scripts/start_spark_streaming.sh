#!/bin/bash
# 逐风数据洞察平台 — Spark Streaming 消费端启动脚本
# 替换 Python 消费端：realtime_consumer.py + satisfaction_consumer.py + hot_char_aggregator.py
# 运行位置：master0（local[2] 模式，不占 Spark Worker 资源）

set -e
SPARK_HOME=/root/spark-2.4.0
JAR=/root/abyss-pipeline/Character-Build-Generator/build-generator.jar
JARS_DIR=/root/abyss-pipeline/jars
JARS=$JARS_DIR/jedis-2.9.0.jar,$JARS_DIR/kafka-clients-2.1.0.jar,$JARS_DIR/spark-streaming-kafka-0-10_2.11-2.4.0.jar,$JARS_DIR/commons-pool2-2.4.2.jar
export LANG=en_US.UTF-8

echo "=== 停止旧 Python 消费端 (Middleware) ==="
ssh Middleware "pkill -f realtime_consumer 2>/dev/null; pkill -f hot_char_aggregator 2>/dev/null; pkill -f satisfaction_consumer 2>/dev/null" || true
sleep 1

echo "=== 停止旧 Spark Streaming ==="
pkill -f CharacterBuildStreamingConsumer 2>/dev/null || true
pkill -f SatisfactionStreamingConsumer 2>/dev/null || true
sleep 2

echo "=== 启动 Build Streaming (build-v2, batch=1s, local[2]) ==="
nohup $SPARK_HOME/bin/spark-submit \
  --class org.example.streaming.CharacterBuildStreamingConsumer \
  --master 'local[2]' --driver-memory 512m \
  --jars $JARS \
  $JAR Middleware:9092 build-v2 Middleware 6379 1 \
  > /tmp/spark_build.log 2>&1 &
echo "  Build PID: $!"

echo "=== 启动 Satisfaction Streaming (satisfaction-v1, batch=3s, local[2]) ==="
nohup $SPARK_HOME/bin/spark-submit \
  --class org.example.streaming.SatisfactionStreamingConsumer \
  --master 'local[2]' --driver-memory 512m \
  --jars $JARS \
  $JAR Middleware:9092 satisfaction-v1 Middleware 6379 3 \
  > /tmp/spark_sat.log 2>&1 &
echo "  Sat PID: $!"

sleep 10
echo ""
echo "=== 验证 ==="
echo "Spark 进程: $(ps aux | grep SparkSubmit | grep -v grep | wc -l)（期望 2）"
echo ""
echo "Redis 验证:"
echo "  redis-cli -h Middleware LLEN build:recent     # 期望 20"
echo "  redis-cli -h Middleware ZCARD rt:satisfaction:ranking  # 期望 121"
echo ""
echo "日志: tail -f /tmp/spark_build.log"
echo "      tail -f /tmp/spark_sat.log"
echo ""
echo "⚠️  Spark UI: http://master0:4040 和 http://master0:4041"
