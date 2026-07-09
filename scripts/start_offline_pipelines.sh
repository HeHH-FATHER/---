#!/bin/bash
# 逐风数据洞察平台 — 离线管道一键启动
# 运行位置：master0
# 不占用 Spark 资源（纯 Python + Hadoop MR）

set -e
cd /root/abyss-pipeline
export LANG=en_US.UTF-8

echo "=== 停止旧离线管道 ==="
pkill -f preprocess_gacha 2>/dev/null || true
pkill -f load_gacha_to_mysql 2>/dev/null || true
sleep 1

echo "=== 启动抽卡离线管道（每 5 分钟 / 5000 抽） ==="
nohup python3 scripts/preprocess_gacha.py --loop 300 --count 5000 2>/dev/null \
  | python3 scripts/CleanGachaMR.py 2>/dev/null \
  | python3 scripts/ComputeGachaStats.py 5000 2>/dev/null \
  | python3 scripts/load_gacha_to_mysql.py 2>/dev/null \
  > /tmp/gacha_pipeline.log 2>&1 &
echo "  PID: $!"

sleep 3
echo ""
echo "=== 验证 ==="
echo "管道进程: $(ps aux | grep preprocess_gacha | grep -v grep | wc -l)"
echo ""
echo "深渊离线管道（手动运行）："
echo "  bash scripts/run_offline_pipeline.sh --input output/ --version \"v6.6\""
echo ""
echo "抽卡日志: tail -f /tmp/gacha_pipeline.log"
echo "MySQL验证: mysql -hMiddleware -uroot -p123456 abyss_db -e 'SELECT batch_time, five_rate FROM ads_gacha_offline WHERE item_name=\"__ALL__\" ORDER BY batch_time DESC LIMIT 3'"
