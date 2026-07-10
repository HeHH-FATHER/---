#!/bin/bash
# 深渊离线管道 — 循环模式（每10分钟从真实统计文件直接写入 MySQL）
cd /root/abyss-pipeline
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
while true; do
  python3 scripts/load_abyss_direct.py 提瓦特数据/深渊配队汇总.json 2>/dev/null
  echo "[Abyss] $(date +%H:%M:%S) 批次完成"
  sleep 600
done
