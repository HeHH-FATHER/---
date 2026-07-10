#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 逐风数据洞察平台 — 全链路一键启动（含健康监测）
# 运行位置：master0（需要能 SSH 到 Middleware）
# ═══════════════════════════════════════════════════════════════
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
cd /root/abyss-pipeline

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[0;33m'; NC='\033[0m'
PASS="${GRN}✅${NC}"; FAIL="${RED}❌${NC}"; WARN="${YLW}⚠️${NC}"

check() { if [ "$1" -ge "$2" ] 2>/dev/null; then echo -e "$PASS $3"; else echo -e "$FAIL $3 (期望≥$2，实际$1)"; fi; }

echo "╔══════════════════════════════════════════════╗"
echo "║  逐风数据洞察平台 — 全链路启动（含监测）      ║"
echo "╚══════════════════════════════════════════════╝"

# ═══════════════════════════════════════════════
# 第0步：全局清理旧进程（防止重复启动）
# ═══════════════════════════════════════════════
echo ""
echo "【0/7】清理旧进程..."
pkill -9 -f "org.example" 2>/dev/null || true
pkill -9 -f "SparkSubmit" 2>/dev/null || true
pkill -9 -f "CoarseGrainedExecutor" 2>/dev/null || true
pkill -9 -f "run_gacha_loop" 2>/dev/null || true
pkill -9 -f "run_abyss_loop" 2>/dev/null || true
pkill -9 -f "preprocess_gacha" 2>/dev/null || true
pkill -9 -f "monitor_master0" 2>/dev/null || true
ssh Middleware "pkill -9 -f monitor_collector 2>/dev/null" 2>/dev/null || true
sleep 2
echo "  旧进程已清理"

# ═══════════════════════════════════════════════
# 第1步：Middleware 基础服务
# ═══════════════════════════════════════════════
echo ""
echo "【1/7】Middleware 基础服务..."
ssh Middleware "
  systemctl start mysql 2>/dev/null || true
  systemctl start redis-server 2>/dev/null || true
  /root/kafka2/bin/zookeeper-server-start.sh -daemon /root/kafka2/config/zookeeper.properties
  sleep 3
  /root/kafka2/bin/kafka-server-start.sh -daemon /root/kafka2/config/server.properties
  sleep 2
  echo \"MYSQL=\$(systemctl is-active mysql 2>/dev/null)\"
  echo \"REDIS=\$(systemctl is-active redis-server 2>/dev/null)\"
  echo \"KAFKA=\$(ps aux | grep kafka.Kafka | grep -v grep | wc -l)\"
" 2>&1 | while read line; do
  case "\$line" in
    MYSQL=active) echo -e "  $PASS MySQL" ;;
    MYSQL=*)      echo -e "  $FAIL MySQL (\$line)" ;;
    REDIS=active) echo -e "  $PASS Redis" ;;
    REDIS=*)      echo -e "  $FAIL Redis (\$line)" ;;
    KAFKA=*)      check "\${line#KAFKA=}" 1 "Kafka" ;;
  esac
done

# ═══════════════════════════════════════════════
# 第2步：Hadoop + Spark 集群
# ═══════════════════════════════════════════════
echo ""
echo "【2/7】Hadoop + Spark 集群..."
/root/hadoop-2.7.6/sbin/start-dfs.sh 2>&1 | tail -1
/root/hadoop-2.7.6/sbin/start-yarn.sh 2>&1 | tail -1
/usr/local/zookeeper/bin/zkServer.sh start 2>&1 | grep -E 'STARTED|already'
/root/spark-2.4.0/sbin/start-all.sh 2>&1 | tail -1

sleep 5
NN=$(/root/jdk1.8.0_171/bin/jps | grep -c NameNode 2>/dev/null || echo 0)
RM=$(/root/jdk1.8.0_171/bin/jps | grep -c ResourceManager 2>/dev/null || echo 0)
SM=$(/root/jdk1.8.0_171/bin/jps | grep -c Master 2>/dev/null || echo 0)
check "$NN" 1 "NameNode"
check "$RM" 1 "ResourceManager"
check "$SM" 1 "Spark Master"

# ═══════════════════════════════════════════════
# 第3步：Java 生成器
# ═══════════════════════════════════════════════
echo ""
echo "【3/7】Java 生成器..."
bash start_generators.sh 2>&1 | tail -3
sleep 2
GEN=$(ps aux | grep -cE 'build_stats|SatisfactionProducerApp' 2>/dev/null || echo 0)
check "$GEN" 2 "生成器进程"

# ═══════════════════════════════════════════════
# 第4步：离线管道（抽卡 + 深渊）
# ═══════════════════════════════════════════════
echo ""
echo "【4/7】离线管道..."
pkill -f run_gacha_loop 2>/dev/null || true
pkill -f run_abyss_loop 2>/dev/null || true
sleep 1
nohup bash scripts/run_gacha_loop.sh > /tmp/gacha_loop.log 2>&1 &
nohup bash scripts/run_abyss_loop.sh > /tmp/abyss_loop.log 2>&1 &

sleep 3
GC=$(ps aux | grep -c run_gacha_loop 2>/dev/null || echo 0)
AB=$(ps aux | grep -c run_abyss_loop 2>/dev/null || echo 0)
check "$GC" 1 "抽卡循环"
check "$AB" 1 "深渊循环"

# ═══════════════════════════════════════════════
# 第5步：Spark Streaming 消费端
# ═══════════════════════════════════════════════
echo ""
echo "【5/7】Spark Streaming 消费端..."
bash scripts/start_spark_streaming.sh 2>&1 | grep -E 'PID|error|Error' | head -5

sleep 15
SP=$(ps aux | grep -c SparkSubmit 2>/dev/null || echo 0)
check "$SP" 2 "Spark 作业"
# Spark 集群应用数
APPS=$(curl -s http://localhost:8080/json/ 2>/dev/null | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("activeapps",[])))' 2>/dev/null || echo 0)
check "$APPS" 2 "Spark Apps (期望≥2)"

# ═══════════════════════════════════════════════
# 第6步：链路监控
# ═══════════════════════════════════════════════
echo ""
echo "【6/7】链路监控采集..."
pkill -f monitor_master0 2>/dev/null || true
sleep 1
nohup python3 -u scripts/monitor_master0.py --loop > /tmp/monitor_m0.log 2>&1 &

# MW 监控：守护进程（自动重启，死循环保活）
ssh Middleware "pkill -f monitor_collector 2>/dev/null; pkill -f monitor_mw_daemon 2>/dev/null; cd /root/abyss-pipeline && nohup bash scripts/monitor_mw_daemon.sh > /tmp/monitor_daemon.log 2>&1 & disown" 2>/dev/null || true
sleep 3
MW=$(ssh Middleware "ps aux | grep -c monitor_collector" 2>/dev/null || echo 0)

sleep 5
MM=$(ps aux | grep -c monitor_master0 2>/dev/null || echo 0)
check "$MM" 1 "master0 监控"
check "$MW" 1 "Middleware 监控"

# ═══════════════════════════════════════════════
# 汇总
# ═══════════════════════════════════════════════
echo ""
echo "╔══════════════════════════════════════╗"
echo "║       全链路启动监测完成              ║"
echo "╠══════════════════════════════════════╣"
echo "║  生成器:  $GEN  | 离线管道: $GC/$AB   ║"
echo "║  Spark:   $SP  | Apps:     $APPS     ║"
echo "║  监控:    $MM (M0) / $MW (MW)       ║"
echo "╚══════════════════════════════════════╝"
