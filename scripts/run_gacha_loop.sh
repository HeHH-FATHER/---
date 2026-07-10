#!/bin/bash
# 抽卡离线管道 — 循环模式（每5分钟5000抽→MySQL）
cd /root/abyss-pipeline
export LANG=en_US.UTF-8
while true; do
  python3 scripts/preprocess_gacha.py --count 5000 --players 50 2>/dev/null \
    | python3 scripts/CleanGachaMR.py 2>/dev/null \
    | python3 scripts/ComputeGachaStats.py 5000 2>/dev/null \
    | python3 scripts/load_gacha_to_mysql.py 2>/dev/null
  echo "[Gacha] $(date +%H:%M:%S) 批次完成"
  python3 -c "from datetime import datetime; import redis; r=redis.Redis(host='Middleware',decode_responses=True); r.set('gacha:last_batch', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))" 2>/dev/null || true
  sleep 300
done
