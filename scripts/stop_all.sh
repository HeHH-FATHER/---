#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 原神数据决策系统 — 全链路停止
# 顺序：消费者 → 生成器 → 管道 → 监控 → 集群 → 中间件
# ═══════════════════════════════════════════════════════════════
set +e

ok() { echo "  ✅ $1"; }
skip() { echo "  ┄ $1 (无进程)"; }

echo "═══ 停止全链路 ═══"
echo ""

# 1. Spark Streaming
echo "[1/6] Spark Streaming..."
n=$(pkill -9 -f "SparkSubmit" 2>/dev/null; echo $?)
[ $n -eq 0 ] && ok "消费端" || skip "消费端"

# 2. Java 生成器
echo "[2/6] 生成器..."
n=$(pkill -9 -f "org.example" 2>/dev/null; echo $?)
[ $n -eq 0 ] && ok "生成器" || skip "生成器"

# 3. 离线循环管道
echo "[3/6] 离线管道..."
pkill -9 -f "run_gacha_loop" 2>/dev/null && ok "抽卡循环" || skip "抽卡循环"
pkill -9 -f "run_abyss_loop" 2>/dev/null && ok "深渊循环" || skip "深渊循环"

# 4. 监控采集
echo "[4/6] 监控..."
pkill -9 -f "monitor_master0" 2>/dev/null && ok "master0 监控" || skip "master0 监控"
ssh Middleware "pkill -9 -f monitor_collector 2>/dev/null" 2>/dev/null && ok "MW 监控" || true

# 5. 集群
echo "[5/6] Spark + Hadoop + ZK..."
/root/spark-2.4.0/sbin/stop-all.sh >/dev/null 2>&1 && ok "Spark"
/root/hadoop-2.7.6/sbin/stop-yarn.sh >/dev/null 2>&1 && ok "YARN"
/root/hadoop-2.7.6/sbin/stop-dfs.sh >/dev/null 2>&1 && ok "HDFS"
/usr/local/zookeeper/bin/zkServer.sh stop >/dev/null 2>&1 && ok "ZooKeeper"

# 6. Middleware
echo "[6/6] Middleware..."
ssh Middleware "
  /root/kafka2/bin/kafka-server-stop.sh 2>/dev/null
  /root/kafka2/bin/zookeeper-server-stop.sh 2>/dev/null
  systemctl stop mysql 2>/dev/null
  systemctl stop redis-server 2>/dev/null
  echo '  ✅ Kafka + ZK + MySQL + Redis'
" 2>/dev/null

# 7. vserver
echo "[+] vserver..."
ssh root@vserver "pkill -f ruoyi-admin 2>/dev/null" 2>/dev/null && ok "vserver 后端" || skip "vserver 后端"

echo ""
echo "═══ 全链路已停止 ═══"
