#!/bin/bash
# 深渊离线管道 — 循环模式（每10分钟：Java生成器+基底融合→MySQL）
cd /root/abyss-pipeline
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
JAVA=/root/jdk1.8.0_171/bin/java
set +e
while true; do
  python3 scripts/build_abyss_stats.py 提瓦特数据 2>/dev/null || true
  rm -rf output/abyss 2>/dev/null

  # 生成器输出（不用quiet，抓脏数据计数）
  GEN_OUT=$($JAVA -Dfile.encoding=UTF-8 -Xmx256m -jar Abyss-Record-Generator/abyss-generator.jar \
    --stats abyss_stats.json --out output/abyss/ --users 200 --dirty-rate 0.05 2>&1) || true
  DIRTY_USERS=$(echo "$GEN_OUT" | grep '脏数据用户' | grep -o '[0-9]\+$' || echo 0)
  ODS_FILES=$(ls output/abyss/ 2>/dev/null | wc -l)

  python3 scripts/preprocess_abyss.py output/abyss/ --version "v6.6" -q -o /tmp/abyss_v66.jsonl 2>/dev/null || true
  python3 scripts/load_abyss_pipeline.py output/abyss/ 2>/dev/null || true

  DIRTY_FILES=$((DIRTY_USERS * 2))
  echo "[Abyss] $(date +%H:%M:%S) ODS=$ODS_FILES 脏用户=$DIRTY_USERS 脏文件=$DIRTY_FILES 批次完成"

  python3 scripts/abyss_redis_update.py $ODS_FILES $DIRTY_USERS 2>/dev/null || true

  sleep 600
done
