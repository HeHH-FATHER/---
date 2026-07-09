#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 逐风数据洞察平台 — 全链路一键停止
# 运行位置：master0
# ═══════════════════════════════════════════════════════════════
echo "=== 停止 Spark Streaming ==="
pkill -9 -f "SparkSubmit" 2>/dev/null && echo "  Spark ✅" || echo "  Spark (无)"

echo "=== 停止 Java 生成器 ==="
pkill -9 -f "org.example" 2>/dev/null && echo "  生成器 ✅" || echo "  生成器 (无)"

echo "=== 停止离线管道 ==="
pkill -9 -f "run_gacha_loop" 2>/dev/null || true
pkill -9 -f "run_abyss_loop" 2>/dev/null || true
pkill -9 -f "preprocess_gacha" 2>/dev/null || true
echo "  离线管道 ✅"

echo "=== 停止监控 ==="
pkill -9 -f "monitor_master0" 2>/dev/null || true
ssh Middleware "pkill -9 -f monitor_collector 2>/dev/null" 2>/dev/null || true
echo "  监控 ✅"

echo "=== 停止 Spark 集群 ==="
/root/spark-2.4.0/sbin/stop-all.sh 2>&1 | tail -1

echo "=== 停止 Hadoop ==="
/root/hadoop-2.7.6/sbin/stop-yarn.sh 2>&1 | tail -1
/root/hadoop-2.7.6/sbin/stop-dfs.sh 2>&1 | tail -1

echo "=== 停止 Zookeeper ==="
/usr/local/zookeeper/bin/zkServer.sh stop 2>&1 | grep -E 'STOPPED|already'

echo "=== 停止 Middleware 服务 ==="
ssh Middleware "
  /root/kafka2/bin/kafka-server-stop.sh 2>/dev/null
  /root/kafka2/bin/zookeeper-server-stop.sh 2>/dev/null
  systemctl stop mysql redis-server 2>/dev/null
" 2>/dev/null
echo "  Kafka + ZK + MySQL + Redis ✅"

echo ""
echo "全链路已停止。"
