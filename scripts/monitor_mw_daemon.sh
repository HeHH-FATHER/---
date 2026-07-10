#!/bin/bash
# Middleware 监控守护进程 — 死循环自动重启，确保永远在线
cd /root/abyss-pipeline
while true; do
  pkill -f monitor_collector 2>/dev/null
  python3 -u scripts/monitor_collector.py --loop
  echo "[MW-Daemon] monitor_collector 退出，5秒后重启..."
  sleep 5
done
