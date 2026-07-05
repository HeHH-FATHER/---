#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 抽卡记录生成器
输入：提瓦特数据/卡池统计_clean.json (102期真实卡池统计)
输出：模拟单条抽卡记录 CSV → HDFS /data/gacha/

用法：
  python gacha_producer.py                 # 默认 100万条
  python gacha_producer.py --scale 500000  # 50万条
  python gacha_producer.py --upload        # 生成并上传HDFS
"""

import json
import csv
import os
import sys
import random
import subprocess
from datetime import datetime
from gacha_common import load_banners, pick_char

# ==================== 配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

SCALE = 1_000_000  # 默认生成100万条
HDFS_TARGET = "/data/gacha/"
PITY_HARD = 90


def generate(scale=SCALE):
    """主生成逻辑"""
    banners = load_banners()
    print(f"加载 {len(banners)} 期卡池")
    print(f"总抽取次数(原始): {sum(b['total'] for b in banners):,}")
    print(f"采样生成: {scale:,} 条\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "gacha_records.csv")

    # 按权重分配每个卡池的生成数量
    counts = []
    for b in banners:
        n = max(1, int(scale * b["weight"]))
        counts.append(n)

    # 补齐差额
    diff = scale - sum(counts)
    for i in range(abs(diff)):
        idx = i % len(banners)
        counts[idx] += 1 if diff > 0 else -1

    base_uid = 100000000
    base_ts = int(datetime(2022, 8, 24).timestamp())

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["uid", "banner", "item", "star", "timestamp", "pity_count"])

        total = 0
        for i, banner in enumerate(banners):
            n = counts[i]
            try:
                t_start = int(datetime.strptime(banner["start"], "%Y-%m-%d").timestamp())
                t_end = int(datetime.strptime(banner["end"], "%Y-%m-%d").timestamp())
            except:
                t_start = base_ts
                t_end = base_ts + 86400 * 21

            pity = 0
            for j in range(n):
                uid = base_uid + random.randint(0, 99999999)
                ts = random.randint(t_start, t_end)
                char = pick_char(banner)
                star = 5 if random.random() < 0.05 else 4
                pity += 1
                if pity >= PITY_HARD:
                    star = 5
                    pity = 0

                writer.writerow([uid, banner["version"], char, star, ts, pity])
                total += 1

                if total % 100000 == 0:
                    print(f"  已生成 {total:,} / {scale:,} 条")

    print(f"\n[OK] 输出: {output_path}")
    print(f"  共 {total:,} 条记录, 覆盖 {len(banners)} 期卡池")
    return output_path


def upload_to_hdfs(local_path):
    """上传到HDFS"""
    hdfs_path = HDFS_TARGET.rstrip("/") + "/" + os.path.basename(local_path)
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_TARGET], check=False, capture_output=True)
        result = subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
                                check=False, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  [HDFS] [OK] → {hdfs_path}")
        else:
            print(f"  [HDFS] [ERR] {result.stderr}")
    except FileNotFoundError:
        print(f"  [HDFS] [WARN] hdfs 命令不可用，文件已保存到本地")


def main():
    global SCALE
    if "--scale" in sys.argv:
        idx = sys.argv.index("--scale") + 1
        SCALE = int(sys.argv[idx])
    do_upload = "--upload" in sys.argv

    print("=" * 50)
    print(f"抽卡记录生成器 (采样 {SCALE:,} 条)")
    print("=" * 50)

    path = generate(SCALE)
    if do_upload:
        upload_to_hdfs(path)


if __name__ == "__main__":
    main()
