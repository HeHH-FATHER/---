#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 原神数据决策系统 — 全链路一键启动
# 运行：master0（自动 SSH 到 Middleware）
# ═══════════════════════════════════════════════════════════════
set +e

ok() { echo "  ✅ $1"; }
warn() { echo "  ⚠️  $1"; }
fail() { echo "  ❌ $1"; }
check() { [ "$1" -ge "$2" ] && ok "$3 ($1)" || warn "$3 不足（期望≥$2，实际 $1）"; }

echo "═══ 原神数据决策系统 · 全链路启动 ═══"
echo ""

# ═══════════════════════════════════════════════════
# 第1步：清理旧进程
# ═══════════════════════════════════════════════════
echo "[1/7] 清理旧进程..."
pkill -9 -f "SparkSubmit" 2>/dev/null || true
pkill -9 -f "org.example" 2>/dev/null || true
pkill -9 -f "run_gacha_loop" 2>/dev/null || true
pkill -9 -f "run_abyss_loop" 2>/dev/null || true
pkill -9 -f "monitor_master0" 2>/dev/null || true
ssh Middleware "pkill -9 -f monitor_collector 2>/dev/null" 2>/dev/null || true
sleep 2
ok "完成"

# ═══════════════════════════════════════════════════
# 第2步：Middleware 基础服务
# ═══════════════════════════════════════════════════
echo "[2/7] Middleware 基础服务..."
ssh Middleware "
  systemctl start mysql 2>/dev/null
  systemctl start redis-server 2>/dev/null
  /root/kafka2/bin/zookeeper-server-start.sh -daemon /root/kafka2/config/zookeeper.properties
  sleep 3
  /root/kafka2/bin/kafka-server-start.sh -daemon /root/kafka2/config/server.properties
  sleep 2
  MYSQL=\$(systemctl is-active mysql 2>/dev/null)
  REDIS=\$(systemctl is-active redis-server 2>/dev/null)
  KAFKA=\$(ps aux | grep -c 'kafka\.Kafka' 2>/dev/null || echo 0)
  ZK=\$(ps aux | grep -c QuorumPeerMain 2>/dev/null || echo 0)
  echo \"MYSQL=\$MYSQL REDIS=\$REDIS KAFKA=\$KAFKA ZK=\$ZK\"
" 2>/dev/null
# 确保 redis 库
ssh Middleware "python3 -c 'import redis' 2>/dev/null || pip3 install redis 2>/dev/null" 2>/dev/null || true
python3 -c "import redis" 2>/dev/null || pip3 install redis 2>/dev/null || true
ok "Middleware"

# ═══════════════════════════════════════════════════
# 第3步：Hadoop + Spark 集群
# ═══════════════════════════════════════════════════
echo "[3/7] Hadoop + Spark 集群..."
/root/hadoop-2.7.6/sbin/start-dfs.sh >/dev/null 2>&1
/root/hadoop-2.7.6/sbin/start-yarn.sh >/dev/null 2>&1
/usr/local/zookeeper/bin/zkServer.sh start >/dev/null 2>&1
/root/spark-2.4.0/sbin/start-master.sh >/dev/null 2>&1
sleep 2
/root/spark-2.4.0/sbin/start-slaves.sh >/dev/null 2>&1
sleep 3
NN=$(/root/jdk1.8.0_171/bin/jps | grep -c NameNode 2>/dev/null || echo 0)
RM=$(/root/jdk1.8.0_171/bin/jps | grep -c ResourceManager 2>/dev/null || echo 0)
check "$NN" 1 "NameNode"
check "$RM" 1 "ResourceManager"

# ═══════════════════════════════════════════════════
# 第4步：Java 生成器
# ═══════════════════════════════════════════════════
echo "[4/7] Java 生成器..."
JAVA=/root/jdk1.8.0_171/bin/java
CP="Character-Build-Generator/build-generator.jar:lib/*:jars/*"
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
pkill -f org.example.App 2>/dev/null || true
pkill -f SatisfactionProducerApp 2>/dev/null || true
sleep 1
$JAVA -Dfile.encoding=UTF-8 -cp $CP org.example.App build_stats.json --count 50 --kafka-bootstrap-servers Middleware:9092 --kafka-topic build-v2 --loop > /tmp/build_gen.log 2>&1 &
disown
$JAVA -Dfile.encoding=UTF-8 -cp $CP org.example.satisfaction.SatisfactionProducerApp 提瓦特数据/角色满意度排行.json --kafka-bootstrap-servers Middleware:9092 --kafka-topic satisfaction-v1 --loop --interval 3 > /tmp/satisfaction.log 2>&1 &
sleep 3
GEN=$(ps aux | grep -c "org.example.App\|SatisfactionProducerApp" 2>/dev/null || echo 0)
check "$GEN" 2 "生成器(练度+满意度)"

# ═══════════════════════════════════════════════════
# 第5步：离线循环管道
# ═══════════════════════════════════════════════════
echo "[5/7] 离线管道（抽卡每5min + 深渊每10min）..."
pkill -f run_gacha_loop 2>/dev/null || true
pkill -f run_abyss_loop 2>/dev/null || true
sleep 1
nohup bash scripts/run_gacha_loop.sh > /tmp/gacha_loop.log 2>&1 &
disown
nohup bash scripts/run_abyss_loop.sh > /tmp/abyss_loop.log 2>&1 &
sleep 3
GACHA_RUN=$(ps aux | grep -c run_gacha_loop 2>/dev/null || echo 0)
ABYSS_RUN=$(ps aux | grep -c run_abyss_loop 2>/dev/null || echo 0)
check "$GACHA_RUN" 1 "抽卡循环"
check "$ABYSS_RUN" 1 "深渊循环"

# ═══════════════════════════════════════════════════
# 第6步：Spark Streaming 消费端
# ═══════════════════════════════════════════════════
echo "[6/7] Spark Streaming..."
rm -rf /tmp/spark-build-v2-checkpoint 2>/dev/null
rm -rf /tmp/spark-satisfaction-checkpoint 2>/dev/null
bash scripts/start_spark_streaming.sh >/dev/null 2>&1
sleep 12
SP=$(ps aux | grep -c SparkSubmit 2>/dev/null || echo 0)
check "$SP" 2 "Spark 作业"

# ═══════════════════════════════════════════════════
# 第7步：链路监控
# ═══════════════════════════════════════════════════
echo "[7/7] 链路监控..."

# 先写初始信标进 Redis
python3 -c "
import redis
r = redis.Redis(host='Middleware', port=6379, decode_responses=True)
r.hset('screen:monitor', mapping={
    'generator_count': 0, 'streaming_count': 0,
    'spark_workers': 0, 'spark_apps': 0, 'hdfs_usage': 0,
    'm0_cpu': 0, 'm0_mem': 0, 'mw_cpu': 0, 'mw_mem': 0,
    'slave1_cpu': 0, 'slave1_mem': 0, 'slave2_cpu': 0, 'slave2_mem': 0,
    'backup_cpu': 0, 'backup_mem': 0, 'vserver_cpu': 0, 'vserver_mem': 0,
    'kafka_build_offset': 0, 'kafka_sat_offset': 0,
    'redis_mem_mb': 0, 'redis_ops': 0, 'redis_keys': 0, 'redis_hit_rate': 100
})
r.expire('screen:monitor', 300)
print('  信标已写入')
" 2>/dev/null || true

# 启动 M0 监控
pkill -f monitor_master0 2>/dev/null || true
sleep 1
nohup python3 -u scripts/monitor_master0.py --loop > /tmp/monitor_m0.log 2>&1 &
sleep 3
M0_MON=$(ps aux | grep -c monitor_master0 2>/dev/null || echo 0)
check "$M0_MON" 1 "master0 监控"

# 启动 MW 监控
ssh Middleware "
  pip3 install redis 2>/dev/null
  pkill -f monitor_collector 2>/dev/null
  pkill -f monitor_mw_daemon 2>/dev/null
  sleep 1
  cd /root/abyss-pipeline && nohup bash scripts/monitor_mw_daemon.sh > /tmp/monitor_daemon.log 2>&1 &
  disown
" 2>/dev/null || true
sleep 5
MW_MON=$(ssh Middleware "ps aux | grep -c monitor_collector" 2>/dev/null || echo 0)
check "$MW_MON" 1 "MW 监控"

# 验证 Redis 数据
sleep 3
MON_DATA=$(python3 -c "
import redis
r = redis.Redis(host='Middleware', port=6379, decode_responses=True)
print(len(r.hgetall('screen:monitor')))
" 2>/dev/null || echo 0)
[ "$MON_DATA" -gt 5 ] && ok "监控数据已就绪（$MON_DATA 字段）" || warn "监控字段偏少（$MON_DATA）"

# ═══════════════════════════════════════════════════
# vserver：启动生产后端
# ═══════════════════════════════════════════════════
echo ""
echo "[+] vserver 后端..."
ssh root@vserver "
  systemctl is-active nginx >/dev/null 2>&1 || systemctl start nginx
  pkill -f ruoyi-admin 2>/dev/null
  sleep 2
  cd /opt/ruoyi && nohup java -Xms256m -Xmx512m -jar ruoyi-admin.jar > /tmp/ruoyi.log 2>&1 &
  disown
" 2>/dev/null
sleep 8
VSVR=$(ssh root@vserver "ps aux | grep -c ruoyi-admin" 2>/dev/null || echo 0)
check "$VSVR" 1 "vserver 后端"
# vserver AI 代理（master0 → 外网）
pgrep -f "proxy.*3128" >/dev/null 2>&1 || nohup python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.request, ssl
class P(BaseHTTPRequestHandler):
    def do_POST(s):
        b = s.rfile.read(int(s.headers.get('Content-Length',0)))
        r = urllib.request.Request('https://api.siliconflow.cn/v1/chat/completions', data=b, headers={'Content-Type':'application/json','Authorization':s.headers.get('Authorization','')})
        try:
            resp = urllib.request.urlopen(r, context=ssl.create_default_context(), timeout=120)
            s.send_response(resp.status); s.send_header('Content-Type','application/json'); s.end_headers(); s.wfile.write(resp.read())
        except Exception as e: s.send_response(502); s.end_headers()
HTTPServer(('0.0.0.0',3128), P).serve_forever()
" > /tmp/proxy.log 2>&1 &
ok "AI 代理 (master0:3128 → SiliconFlow)"

if [ "$VSVR" -ge 1 ]; then
  sleep 3
  VSVR_API=$(ssh root@vserver "curl -s http://localhost:8080/analysis/abyss/quality | head -c 20" 2>/dev/null)
  [ -n "$VSVR_API" ] && ok "vserver API 就绪" || warn "vserver API 启动中"
fi

# ═══════════════════════════════════════════════════
# 汇总
# ═══════════════════════════════════════════════════
echo ""
echo "═══ 启动完成 ═══"
echo "大屏: http://100.84.184.73:80/ (local)"
echo "      http://100.74.215.12/ (vserver)"
echo "Spark: http://master0:8080"
echo "日志: /tmp/*.log"
