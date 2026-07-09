#!/bin/bash
# 深渊离线管道 — 循环模式（每10分钟 200玩家 → MySQL）
cd /root/abyss-pipeline
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
JAVA=/root/jdk1.8.0_171/bin/java
while true; do
  # ① 构建 StatsDocument JSON（从原始API数据）
  python3 scripts/build_abyss_stats.py 提瓦特数据 2>/dev/null

  # ② Java 生成器：200用户 → per-user JSON
  rm -rf output/abyss
  $JAVA -Dfile.encoding=UTF-8 -Xmx256m -jar Abyss-Record-Generator/abyss-generator.jar \
    --stats abyss_stats.json --out output/abyss/ --users 200 --dirty-rate 0.05 --quiet 2>/dev/null

  # ③ 预处理：合并 BOX+战绩 → JSONL
  python3 scripts/preprocess_abyss.py output/abyss/ --version "v6.6" -q -o /tmp/abyss_v66.jsonl 2>/dev/null

  # ④ 聚合入库
  cat /tmp/abyss_v66.jsonl | python3 scripts/load_abyss_to_mysql.py 2>/dev/null

  echo "[Abyss] $(date +%H:%M:%S) 批次完成"
  sleep 600  # 每10分钟
done
