#!/bin/bash
# 深渊离线管道 — 循环模式（每10分钟：Java生成器+基底融合→MySQL）
cd /root/abyss-pipeline
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
JAVA=/root/jdk1.8.0_171/bin/java
set +e
while true; do
  python3 scripts/build_abyss_stats.py 提瓦特数据 2>/dev/null || true
  rm -rf output/abyss 2>/dev/null
  $JAVA -Dfile.encoding=UTF-8 -Xmx256m -jar Abyss-Record-Generator/abyss-generator.jar \
    --stats abyss_stats.json --out output/abyss/ --users 200 --dirty-rate 0.05 --quiet 2>/dev/null || true
  python3 scripts/preprocess_abyss.py output/abyss/ --version "v6.6" -q -o /tmp/abyss_v66.jsonl 2>/dev/null || true
  python3 scripts/load_abyss_direct.py 提瓦特数据/原始数据/新建文件夹/v59_6.6深渊使用率统计（第一期） - 副本.json 2>/dev/null || true
  echo "[Abyss] $(date +%H:%M:%S) 批次完成"
  python3 -c "import redis; r=redis.Redis(host='Middleware',decode_responses=True); r.set('abyss:last_batch', __import__('datetime').datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))" 2>/dev/null || true
  sleep 600
done
