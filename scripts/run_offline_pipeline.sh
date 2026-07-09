#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 逐风数据洞察平台 — 离线链路一键管道
# ═══════════════════════════════════════════════════════════════
# 层级: ODS(HDFS) → DWD(HDFS) → DWS(HDFS) → ADS(MySQL)
# 执行位置: master0 节点
#
# 用法:
#   bash run_offline_pipeline.sh \
#     --input /path/to/generated/json \
#     --version "v6.6(第一期)" \
#     [--skip-mr-build] [--skip-upload] [--dry-run]
#
# 前提:
#   1. Hadoop 集群已启动 (NameNode + DataNode + ResourceManager)
#   2. MySQL (Middleware) 已启动
#   3. MR Jar 已编译: abyss-mr.jar
# ═══════════════════════════════════════════════════════════════

set -e  # 出错即停

# ═══════════════════════════════════════════
# 集群路径（对照 集群组件安装位置清单.md）
# ═══════════════════════════════════════════
HADOOP_HOME=/root/hadoop-2.7.6
HADOOP_BIN=$HADOOP_HOME/bin
HDFS="$HADOOP_BIN/hdfs dfs"
JAVA_HOME=/root/jdk1.8.0_171
JAVA=$JAVA_HOME/bin/java

# HDFS 分层目录
HDFS_ODS=/data/abyss/ods
HDFS_DWD_CHAR=/data/abyss/dwd/dwd_char_detail
HDFS_DWD_TEAM=/data/abyss/dwd/dwd_team_usage
HDFS_DWS_CHAR=/data/abyss/dws/dws_char_summary
HDFS_DWS_COOCCUR=/data/abyss/dws/dws_team_cooccur
HDFS_DIRTY=/data/abyss/dirty

# MySQL
MYSQL_HOST=100.103.177.85
MYSQL_USER=root
MYSQL_PASS=123456
MYSQL_DB=abyss_db

# MR Jar
PROJECT_DIR="/root/abyss-pipeline"
MR_JAR="$PROJECT_DIR/abyss-mr.jar"
MAIN_CLEAN=com.neu.abyss.AbyssCleanMR
MAIN_AGG=com.neu.abyss.AbyssAggMR

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════

log_section() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
}

log_step() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error(){ echo -e "${RED}[✗]${NC} $1"; }

check_prereq() {
    log_section "0. 环境检查"

    # Hadoop
    if $HADOOP_BIN/hdfs dfsadmin -report >/dev/null 2>&1; then
        log_step "HDFS 正常"
    else
        log_error "HDFS 不可用！请先 start-dfs.sh"
        exit 1
    fi

    # YARN
    if $HADOOP_BIN/yarn node -list >/dev/null 2>&1; then
        log_step "YARN 正常"
    else
        log_warn "YARN 不可用（MR 将以 local 模式运行）"
    fi

    # MySQL
    if command -v mysql >/dev/null 2>&1; then
        if mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS -e "SELECT 1" >/dev/null 2>&1; then
            log_step "MySQL ($MYSQL_HOST) 正常"
        else
            log_warn "MySQL 连接失败（ADS 加载将跳过）"
        fi
    else
        log_warn "mysql 客户端未安装"
    fi

    # MR Jar
    if [ -f "$MR_JAR" ]; then
        log_step "MR Jar: $MR_JAR"
    else
        log_error "MR Jar 不存在: $MR_JAR"
        log_error "请先编译: cd $PROJECT_DIR && mvn package -DskipTests"
        exit 1
    fi

    # 输入数据
    if [ -d "$INPUT_DIR" ]; then
        local count=$(ls "$INPUT_DIR"/*_char_box.json 2>/dev/null | wc -l)
        log_step "输入目录: $INPUT_DIR ($count 个用户)"
    else
        log_error "输入目录不存在: $INPUT_DIR"
        exit 1
    fi
}

# ═══════════════════════════════════════════
# 参数解析
# ═══════════════════════════════════════════
INPUT_DIR=""
VERSION=""
SKIP_MR_BUILD=false
SKIP_ADS=false
DRY_RUN=false
CLEAN_HDFS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --input)    INPUT_DIR="$2"; shift 2 ;;
        --version)  VERSION="$2"; shift 2 ;;
        --skip-mr-build) SKIP_MR_BUILD=true; shift ;;
        --skip-ads) SKIP_ADS=true; shift ;;
        --dry-run)  DRY_RUN=true; shift ;;
        --clean-hdfs) CLEAN_HDFS=true; shift ;;
        -h|--help)
            echo "用法: bash run_offline_pipeline.sh --input <dir> --version <ver>"
            echo ""
            echo "四层链路: ODS(HDFS) → DWD(HDFS) → DWS(HDFS) → ADS(MySQL)"
            echo ""
            echo "必填:"
            echo "  --input    生成器 JSON 输出目录"
            echo "  --version  数据版本, 如 v6.6(第一期)"
            echo ""
            echo "可选:"
            echo "  --skip-mr-build  跳过 MR 编译"
            echo "  --skip-ads       跳过 ADS→MySQL 加载"
            echo "  --clean-hdfs     清空旧 HDFS 数据后重新开始"
            echo "  --dry-run        仅打印步骤，不执行"
            exit 0
            ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

if [ -z "$INPUT_DIR" ] || [ -z "$VERSION" ]; then
    log_error "必须指定 --input 和 --version"
    echo "用法: bash run_offline_pipeline.sh --input <dir> --version <ver>"
    exit 1
fi

# ═══════════════════════════════════════════
# 0. 环境检查
# ═══════════════════════════════════════════
check_prereq

if $CLEAN_HDFS && ! $DRY_RUN; then
    log_warn "清空 HDFS 旧数据..."
    $HDFS -rm -r -f -skipTrash /data/abyss/
fi

# 确保 HDFS 目录
$HDFS -mkdir -p $HDFS_ODS $HDFS_DWD_CHAR $HDFS_DWD_TEAM \
                  $HDFS_DWS_CHAR $HDFS_DWS_COOCCUR $HDFS_DIRTY

# ═══════════════════════════════════════════
# 1. ODS 层: 预处理 → HDFS
# ═══════════════════════════════════════════
log_section "1. ODS 层: 数据预处理 (JSON → JSONL → HDFS)"

SAFE_VER=$(echo "$VERSION" | sed 's/[()/]/_/g')
ODS_FILE="abyss_${SAFE_VER}.jsonl"
ODS_LOCAL="/tmp/$ODS_FILE"

if ! $DRY_RUN; then
    log_step "运行 preprocess_abyss.py..."
    python3 $PROJECT_DIR/scripts/preprocess_abyss.py \
        "$INPUT_DIR" \
        --version "$VERSION" \
        --output "$ODS_LOCAL" \
        --quiet

    log_step "上传到 HDFS: $HDFS_ODS/$ODS_FILE"
    $HDFS -put -f "$ODS_LOCAL" "$HDFS_ODS/$ODS_FILE"

    ODS_COUNT=$(wc -l < "$ODS_LOCAL")
    log_step "ODS 就绪: $ODS_COUNT 行 → $HDFS_ODS/$ODS_FILE"
else
    echo "  [DRY-RUN] python3 preprocess_abyss.py $INPUT_DIR --version $VERSION"
    echo "  [DRY-RUN] hdfs dfs -put $ODS_LOCAL $HDFS_ODS/$ODS_FILE"
fi

# ═══════════════════════════════════════════
# 2. DWD 层: AbyssCleanMR (JSONL → CSV)
# ═══════════════════════════════════════════
log_section "2. DWD 层: 数据清洗 MR (10 条规则)"

TMP_CLEAN=/data/abyss/tmp_clean_$$
TMP_DWD_CHAR="$TMP_CLEAN/dwd/char_detail"
TMP_DWD_TEAM="$TMP_CLEAN/dwd/team_usage"
TMP_DIRTY="$TMP_CLEAN/dirty"

if ! $DRY_RUN; then
    log_step "提交 AbyssCleanMR..."
    $HADOOP_BIN/hadoop jar $MR_JAR $MAIN_CLEAN \
        "$HDFS_ODS/$ODS_FILE" \
        "$TMP_CLEAN"

    log_step "移动 DWD 产出到正式目录..."
    # char_detail
    if $HDFS -test -d "$TMP_DWD_CHAR"; then
        $HDFS -rm -r -f -skipTrash "$HDFS_DWD_CHAR"
        $HDFS -mkdir -p "$HDFS_DWD_CHAR"
        $HDFS -cp "$TMP_DWD_CHAR/*" "$HDFS_DWD_CHAR/"
        CHAR_COUNT=$($HDFS -cat "$HDFS_DWD_CHAR/"* 2>/dev/null | wc -l)
        log_step "  dwd_char_detail: $CHAR_COUNT 行"
    fi

    # team_usage
    if $HDFS -test -d "$TMP_DWD_TEAM"; then
        $HDFS -rm -r -f -skipTrash "$HDFS_DWD_TEAM"
        $HDFS -mkdir -p "$HDFS_DWD_TEAM"
        $HDFS -cp "$TMP_DWD_TEAM/*" "$HDFS_DWD_TEAM/"
        TEAM_COUNT=$($HDFS -cat "$HDFS_DWD_TEAM/"* 2>/dev/null | wc -l)
        log_step "  dwd_team_usage: $TEAM_COUNT 行"
    fi

    # dirty 归档
    if $HDFS -test -d "$TMP_DIRTY"; then
        DIRTY_COUNT=$($HDFS -count "$TMP_DIRTY" 2>/dev/null | awk '{print $2}')
        log_step "  脏数据目录: $TMP_DIRTY"
    fi

    # 清理临时目录
    $HDFS -rm -r -f -skipTrash "$TMP_CLEAN"
    log_step "DWD 层完成!"
else
    echo "  [DRY-RUN] hadoop jar $MR_JAR $MAIN_CLEAN $HDFS_ODS/$ODS_FILE $TMP_CLEAN"
fi

# ═══════════════════════════════════════════
# 3. DWS 层: AbyssAggMR + 配队共现
# ═══════════════════════════════════════════
log_section "3. DWS 层: 聚合 MR (DWD CSV → DWS CSV)"

if ! $DRY_RUN; then
    log_step "提交 AbyssAggMR..."
    $HADOOP_BIN/hadoop jar $MR_JAR $MAIN_AGG \
        "$HDFS_DWD_CHAR" \
        "$HDFS_DWS_CHAR"

    DWS_COUNT=$($HDFS -cat "$HDFS_DWS_CHAR/"part-* 2>/dev/null | wc -l)
    log_step "dws_char_summary: $DWS_COUNT 个角色组"

    # 配队共现
    log_step "计算配队共现..."
    python3 $PROJECT_DIR/scripts/compute_team_cooccur.py \
        "$HDFS_DWD_TEAM/" \
        "$HDFS_DWS_COOCCUR/"

    log_step "DWS 层完成!"
else
    echo "  [DRY-RUN] hadoop jar $MR_JAR $MAIN_AGG $HDFS_DWD_CHAR $HDFS_DWS_CHAR"
    echo "  [DRY-RUN] python3 compute_team_cooccur.py $HDFS_DWD_TEAM/ $HDFS_DWS_COOCCUR/"
fi

# ═══════════════════════════════════════════
# 4. ADS 层: MySQL 加载
# ═══════════════════════════════════════════
if $SKIP_ADS; then
    log_warn "跳过 ADS 加载 (--skip-ads)"
else
    log_section "4. ADS 层: HDFS DWS → MySQL ADS"

    if ! $DRY_RUN; then
        log_step "运行 load_ads_to_mysql.py..."
        python3 $PROJECT_DIR/scripts/load_ads_to_mysql.py \
            --version "$VERSION" \
            --host "$MYSQL_HOST" \
            --user "$MYSQL_USER" \
            --password "$MYSQL_PASS" \
            --database "$MYSQL_DB"

        log_step "ADS 层完成!"
    else
        echo "  [DRY-RUN] python3 load_ads_to_mysql.py --version $VERSION"
    fi
fi

# ═══════════════════════════════════════════
# 4.5 版本涨跌异动计算
# ═══════════════════════════════════════════
log_section "4.5 版本涨跌异动 (角色跨版本动量)"

if ! $DRY_RUN; then
    log_step "运行 compute_momentum.py..."
    python3 $PROJECT_DIR/scripts/compute_momentum.py \
        --host "$MYSQL_HOST" \
        --user "$MYSQL_USER" \
        --pass "$MYSQL_PASS" \
        --db "$MYSQL_DB"
    log_step "动量计算完成 → ads_char_momentum"
else
    echo "  [DRY-RUN] python3 compute_momentum.py"
fi

# ═══════════════════════════════════════════
# 5. 摘要
# ═══════════════════════════════════════════
log_section "离线链路完成!"

echo "数据四层流转:"
echo "  ODS: $HDFS_ODS/$ODS_FILE"
echo "  DWD: $HDFS_DWD_CHAR  +  $HDFS_DWD_TEAM"
echo "  DWS: $HDFS_DWS_CHAR  +  $HDFS_DWS_COOCCUR"
echo "  ADS: mysql://$MYSQL_HOST:3306/$MYSQL_DB"
echo ""
echo "验证命令:"
echo "  hdfs dfs -cat $HDFS_DWS_CHAR/part-* | head -5"
echo "  mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e 'SELECT * FROM ads_meta_ranking LIMIT 5'"
echo ""
echo "大屏: http://100.74.215.12:8080/"

exit 0
