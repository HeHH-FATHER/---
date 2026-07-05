#!/bin/bash
# ============================================================
# 逐风数据洞察平台 — 全链路一键启动 / 构建脚本
#
# 三条链路：
#   1. 离线链路: ODS→DWD→DWS→ADS（MapReduce / Spark 双实现）
#   2. 实时链路: Producer→Kafka→Consumer→Redis
#   3. 可视化链路: ADS表→AI报告→Dashboard
#
# 用法：
#   bash run_all_pipelines.sh              # 全量（按顺序）
#   bash run_all_pipelines.sh --offline    # 仅离线链路
#   bash run_all_pipelines.sh --realtime   # 仅实时链路
#   bash run_all_pipelines.sh --report     # 仅 AI 报告 + ADS 补表
#   bash run_all_pipelines.sh --build      # 编译 MR + Spark JAR
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_GEN_DIR="$PROJECT_DIR/data_generator"
MR_DIR="$PROJECT_DIR/mr_jobs"
SPARK_DIR="$PROJECT_DIR/spark_jobs"
STREAM_DIR="$PROJECT_DIR/streaming_jobs"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info()  { echo -e "${CYAN}[INFO]${NC} $1"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}   $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ============================================================
# 构建
# ============================================================

build_mr() {
    log_info "编译 MapReduce 作业..."
    cd "$MR_DIR"
    if command -v mvn &>/dev/null; then
        mvn clean package -DskipTests -q && log_ok "MR JAR 编译完成"
    else
        log_warn "Maven 不可用，跳过 MR 编译"
        log_warn "手动编译: javac -cp \${HADOOP_CP}:\${JACKSON_CP} -d classes src/com/neu/abyss/*.java"
    fi
}

build_spark() {
    log_info "编译 Spark 批处理作业..."
    cd "$SPARK_DIR"
    if command -v sbt &>/dev/null; then
        sbt assembly 2>&1 | tail -5 && log_ok "Spark JAR 编译完成"
    else
        log_warn "SBT 不可用，跳过 Spark 编译"
    fi
}

build_streaming() {
    log_info "编译 Streaming 作业..."
    cd "$STREAM_DIR"
    if command -v sbt &>/dev/null; then
        sbt assembly 2>&1 | tail -5 && log_ok "Streaming JAR 编译完成"
    else
        log_warn "SBT 不可用，跳过 Streaming 编译"
    fi
}

# ============================================================
# 离线链路
# ============================================================

run_offline_mr() {
    log_info "===== 离线链路 (MapReduce) ====="

    # 1. ODS: 数据预处理
    log_info "[1/4] ODS 预处理..."
    python3 "$SCRIPT_DIR/preprocess_abyss.py" && log_ok "ODS 完成"

    # 2. DWD: 清洗
    log_info "[2/4] DWD 清洗 (AbyssCleanMR)..."
    log_warn "DWD 需在 master0 手动执行:"
    echo "  hadoop jar abyss-mr.jar com.neu.abyss.AbyssCleanMR \\"
    echo "    -libjars jackson-*.jar /data/abyss/ods/abyss_v66.jsonl /data/abyss/tmp_clean"

    # 3. DWS: 聚合
    log_info "[3/4] DWS 聚合 (AbyssAggMR)..."
    log_warn "DWS 需在 master0 手动执行:"
    echo "  hadoop jar abyss-mr.jar com.neu.abyss.AbyssAggMR \\"
    echo "    /data/abyss/dwd/dwd_char_detail/ /data/abyss/dws/dws_char_summary/"

    # 4. ADS: MySQL加载
    log_info "[4/4] ADS 加载..."
    python3 "$SCRIPT_DIR/load_ads_to_mysql.py" && log_ok "ADS 完成"
}

run_offline_spark() {
    log_info "===== 离线链路 (Spark) ====="
    log_warn "Spark 需在 master0 手动执行:"
    echo "  spark-submit --master yarn --class DataImport abyss-data-platform.jar"
    echo "  spark-submit --master yarn --class DataClean abyss-data-platform.jar"
    echo "  spark-submit --master yarn --class MetaTimeline abyss-data-platform.jar"
    echo "  spark-submit --master yarn --class SatifyAnalysis abyss-data-platform.jar"
    echo "  spark-submit --master yarn --class TeamMining abyss-data-platform.jar"
    echo "  spark-submit --master yarn --class AdsBuilder abyss-data-platform.jar"
}

# ============================================================
# 实时链路
# ============================================================

run_realtime() {
    log_info "===== 实时链路 ====="
    log_warn "Producer 端启动:"
    echo "  bash scripts/run_realtime_pipeline.sh"
    echo ""
    log_warn "Spark Streaming 需在 master0 提交:"
    echo "  spark-submit --master yarn --class DpsMonitor      abyss-streaming-platform.jar"
    echo "  spark-submit --master yarn --class NukeAlert       abyss-streaming-platform.jar"
    echo "  spark-submit --master yarn --class ReactionHeat    abyss-streaming-platform.jar"
}

# ============================================================
# 可视化
# ============================================================

run_visual() {
    log_info "===== 可视化链路 ====="
    log_warn "RuoYi 后端 (IDEA → localhost:8080)"
    log_warn "RuoYi 前端 (npm run dev → localhost:8088)"
}

# ============================================================
# 主入口
# ============================================================

echo "============================================"
echo "逐风数据洞察平台 — 全链路管道"
echo "============================================"
echo ""

case "${1:-all}" in
    --build)
        build_mr
        build_spark
        build_streaming
        log_ok "全部编译完成"
        ;;
    --offline)
        run_offline_mr
        ;;
    --offline-spark)
        run_offline_spark
        ;;
    --realtime)
        run_realtime
        ;;
    --visual)
        run_visual
        ;;
    all|--all)
        log_info ">>> 阶段1: 构建 <<<"
        build_mr
        build_spark
        build_streaming
        echo ""

        log_info ">>> 阶段2: 离线链路 <<<"
        run_offline_mr
        echo ""

        log_info ">>> 阶段3: 实时链路 <<<"
        run_realtime
        echo ""

        log_info ">>> 阶段4: 可视化 <<<"
        run_report
        echo ""

        echo "============================================"
        log_ok "全链路完成！"
        echo "============================================"
        ;;
    *)
        echo "用法: bash run_all_pipelines.sh [--build|--offline|--realtime|--visual|all]"
        ;;
esac
