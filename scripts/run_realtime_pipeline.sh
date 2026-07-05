#!/bin/bash
# ============================================================
# 逐风数据洞察平台 — 实时链路 Kafka Producer 启动脚本
#
# 链路: Producer → Kafka(Middleware) → Spark Streaming(master0) → Redis(Middleware)
# 本脚本只启动 Producer 端，Spark Streaming 需在 master0 单独提交。
#
# 前置条件:
#   1. Middleware 节点 ZK + Kafka 已启动
#   2. Python 3 + kafka-python 已安装
#
# 用法:
#   bash run_realtime_pipeline.sh              # 全部启动（后台）
#   bash run_realtime_pipeline.sh --stop       # 停止全部
#   bash run_realtime_pipeline.sh --status     # 查看状态
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_GEN_DIR="$(dirname "$SCRIPT_DIR")/data_generator"
PID_DIR="/tmp/abyss-pipeline"
mkdir -p "$PID_DIR"

KAFKA_BROKER="Middleware:9092"
TOPIC_GACHA="gacha_log"
TOPIC_BUILD="build_log"

# ==================== 启动 ====================

start_gacha_producer() {
    echo "[启动] gacha_producer → Kafka $TOPIC_GACHA"
    cd "$DATA_GEN_DIR"
    nohup python3 gacha_kafka_producer.py > "$PID_DIR/gacha_producer.log" 2>&1 &
    echo $! > "$PID_DIR/gacha_producer.pid"
    echo "  PID: $(cat $PID_DIR/gacha_producer.pid)"
}

start_build_producer() {
    echo "[启动] build_producer → Kafka $TOPIC_BUILD"
    cd "$DATA_GEN_DIR"
    nohup python3 build_producer.py kafka > "$PID_DIR/build_producer.log" 2>&1 &
    echo $! > "$PID_DIR/build_producer.pid"
    echo "  PID: $(cat $PID_DIR/build_producer.pid)"
}

# ==================== 停止 ====================

stop_all() {
    echo "停止 Kafka Producer..."
    for pidfile in "$PID_DIR"/*.pid; do
        if [ -f "$pidfile" ]; then
            name=$(basename "$pidfile" .pid)
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null && echo "  [OK] $name (PID: $pid) 已停止"
            else
                echo "  [SKIP] $name (PID: $pid) 已不在运行"
            fi
            rm -f "$pidfile"
        fi
    done
    echo "全部停止"
}

show_status() {
    echo "Kafka Producer 状态:"
    echo "---"
    for pidfile in "$PID_DIR"/*.pid; do
        if [ -f "$pidfile" ]; then
            name=$(basename "$pidfile" .pid)
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  ✅ $name (PID: $pid) 运行中"
            else
                echo "  ❌ $name (PID: $pid) 已退出"
            fi
        fi
    done
}

# ==================== 主入口 ====================

case "${1:-start}" in
    --stop)
        stop_all
        ;;
    --status)
        show_status
        ;;
    start|--start)
        echo "============================================"
        echo "逐风数据洞察平台 — 实时链路 Producer 启动"
        echo "============================================"
        echo "  Kafka:  $KAFKA_BROKER"
        echo "  Topics: $TOPIC_GACHA, $TOPIC_BUILD"
        echo "============================================"
        echo ""

        stop_all 2>/dev/null || true
        sleep 1

        start_gacha_producer
        sleep 2
        start_build_producer

        echo ""
        echo "Producer 已启动。Spark Streaming 需在 master0 提交："
        echo "  spark-submit --master yarn --class GachaAggregator  abyss-streaming-platform.jar"
        echo "  spark-submit --master yarn --class BuildAggregator       abyss-streaming-platform.jar"
        echo ""
        echo "停止: bash $0 --stop"
        ;;
    *)
        echo "用法: bash $0 [start|--stop|--status]"
        ;;
esac
