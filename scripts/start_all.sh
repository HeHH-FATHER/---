#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 逐风数据洞察平台 — 全链路一键启动
# 运行位置：master0（需要能 SSH 到 Middleware）
# ═══════════════════════════════════════════════════════════════
set -e
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
cd /root/abyss-pipeline

echo "╔══════════════════════════════════════════════╗"
echo "║  逐风数据洞察平台 — 全链路启动               ║"
echo "╚══════════════════════════════════════════════╝"

# ═══════════════════════════════════════════════
# 第1步：Middleware 基础服务
# ═══════════════════════════════════════════════
echo ""
echo "【1/6】Middleware 基础服务..."
ssh Middleware "
  systemctl start mysql 2>/dev/null || true
  systemctl start redis-server 2>/dev/null || true
  /root/kafka2/bin/zookeeper-server-start.sh -daemon /root/kafka2/config/zookeeper.properties
  sleep 3
  /root/kafka2/bin/kafka-server-start.sh -daemon /root/kafka2/config/server.properties
" && echo "  MySQL + Redis + ZK + Kafka ✅"

# ═══════════════════════════════════════════════
# 第2步：Hadoop + Spark 集群
# ═══════════════════════════════════════════════
echo ""
echo "【2/6】Hadoop + Spark 集群..."
/root/hadoop-2.7.6/sbin/start-dfs.sh
/root/hadoop-2.7.6/sbin/start-yarn.sh
/usr/local/zookeeper/bin/zkServer.sh start
/root/spark-2.4.0/sbin/start-all.sh
echo "  HDFS + YARN + Spark ✅"

# ═══════════════════════════════════════════════
# 第3步：Java 生成器
# ═══════════════════════════════════════════════
echo ""
echo "【3/6】Java 生成器..."
bash start_generators.sh
echo "  练度 + 满意度 ✅"

# ═══════════════════════════════════════════════
# 第4步：离线管道（抽卡循环 + 深渊循环）
# ═══════════════════════════════════════════════
echo ""
echo "【4/6】离线管道..."
# 抽卡
pkill -f run_gacha_loop 2>/dev/null || true
nohup bash scripts/run_gacha_loop.sh > /tmp/gacha_loop.log 2>&1 &
echo "  抽卡循环 ✅ （每5分钟）"

# 深渊
pkill -f run_abyss_loop 2>/dev/null || true
nohup bash scripts/run_abyss_loop.sh > /tmp/abyss_loop.log 2>&1 &
echo "  深渊循环 ✅ （每10分钟）"

# ═══════════════════════════════════════════════
# 第5步：Spark Streaming 消费端
# ═══════════════════════════════════════════════
echo ""
echo "【5/6】Spark Streaming 消费端..."
bash scripts/start_spark_streaming.sh
echo "  Spark Streaming ✅"

# ═══════════════════════════════════════════════
# 第6步：链路监控
# ═══════════════════════════════════════════════
echo ""
echo "【6/6】链路监控采集..."
ssh Middleware "pkill -f monitor_collector 2>/dev/null; cd /root/abyss-pipeline && nohup python3 -u scripts/monitor_collector.py --loop > /tmp/monitor.log 2>&1 & disown" 2>/dev/null || true
pkill -f monitor_master0 2>/dev/null || true
sleep 1
nohup python3 -u scripts/monitor_master0.py --loop > /tmp/monitor_m0.log 2>&1 &
echo "  6节点监控 ✅"

# ═══════════════════════════════════════════════
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  全链路启动完成！                            ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  验证命令:                                   ║"
echo "║    ps aux | grep -E 'SparkSubmit|java|python' ║"
echo "║    curl -s http://localhost:8080/json/       ║"
echo "║    redis-cli -h Middleware LLEN build:recent ║"
echo "╚══════════════════════════════════════════════╝"
