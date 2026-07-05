#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — DWS 层 配队共现计算
═══════════════════════════════════════════════════════════════
功能: 从 DWD 层 dwd_team_usage.csv 计算角色共现次数（用于网络图）

层级: DWS（HDFS）— 聚合汇总
下游: ADS 层 SQL → ads_team_network

输入: /data/abyss/dwd/dwd_team_usage/
      CSV: version,uid,team_half,team_index,char_name,star,position

输出: /data/abyss/dws/dws_team_cooccur/
      CSV: char_a,char_b,cooccur_count

用法:
  python compute_team_cooccur.py [--hdfs-input /data/abyss/dwd/dwd_team_usage/]
                                 [--hdfs-output /data/abyss/dws/dws_team_cooccur/]
═══════════════════════════════════════════════════════════════
"""

import subprocess
import sys
import os
import tempfile
from collections import defaultdict


def run_hdfs_cat(hdfs_path):
    """读取 HDFS 目录下所有 part 文件内容"""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_path + "part-*", hdfs_path + "data-*",
             hdfs_path + "*/part-*", hdfs_path + "*/data-*"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            # 尝试不带通配符
            result = subprocess.run(
                ["hdfs", "dfs", "-cat", hdfs_path + "/*"],
                capture_output=True, text=True, timeout=300
            )
        return result.stdout
    except Exception as e:
        print(f"[ERROR] HDFS 读取失败: {e}")
        return None


def compute_cooccurrence(lines_iter):
    """
    将 dwd_team_usage 行按队伍分组，计算队内角色两两共现

    输入: version,uid,team_half,team_index,char_name,star,position

    算法: 同一个 (version, uid, team_half, team_index) 的所有角色属于同一队伍
    """
    # {(version, uid, team_half, team_index): [char_name, ...]}
    teams = defaultdict(list)

    for line in lines_iter:
        line = line.strip()
        if not line or line.startswith("version"):
            continue

        parts = line.split(",", 6)
        if len(parts) < 5:
            continue

        version   = parts[0].strip()
        uid       = parts[1].strip()
        team_half = parts[2].strip()
        team_idx  = parts[3].strip()
        char_name = parts[4].strip()

        team_key = (version, uid, team_half, team_idx)
        teams[team_key].append(char_name)

    # 计算队内角色对共现次数
    pair_counts = defaultdict(int)

    for team_key, chars in teams.items():
        # 去重（同一角色不应在同一队出现两次，但以防万一）
        unique_chars = list(set(chars))
        n = len(unique_chars)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = unique_chars[i], unique_chars[j]
                # 字母序保证 (A,B) 和 (B,A) 合并为同一个对
                if a < b:
                    pair_counts[(a, b)] += 1
                else:
                    pair_counts[(b, a)] += 1

    return pair_counts


def main():
    hdfs_input = sys.argv[1] if len(sys.argv) > 1 else "/data/abyss/dwd/dwd_team_usage/"
    hdfs_output = sys.argv[2] if len(sys.argv) > 2 else "/data/abyss/dws/dws_team_cooccur/"

    print("=" * 60)
    print("逐风数据洞察平台 — DWS 层 配队共现计算")
    print("=" * 60)
    print(f"输入: {hdfs_input}")
    print(f"输出: {hdfs_output}")

    # 1. 从 HDFS 读取
    print("\n[1/3] 读取 DWD 配队数据...")
    raw = run_hdfs_cat(hdfs_input)
    if raw is None or not raw.strip():
        print("[ERROR] 无法读取 HDFS 数据，请确认路径正确且 MR 已运行")
        sys.exit(1)

    lines = raw.strip().split("\n")
    print(f"  读取 {len(lines)} 行")

    # 2. 计算共现
    print("\n[2/3] 计算角色对共现...")
    pair_counts = compute_cooccurrence(lines)
    print(f"  共 {len(pair_counts)} 个角色对")

    # 3. 输出 CSV → HDFS
    print("\n[3/3] 输出到 HDFS...")

    # 写本地临时文件
    tmpfile = tempfile.mktemp(suffix=".csv")
    with open(tmpfile, "w", encoding="utf-8") as f:
        f.write("char_a,char_b,cooccur_count\n")
        for (a, b), count in sorted(pair_counts.items(), key=lambda x: -x[1]):
            f.write(f"{a},{b},{count}\n")

    # 上传 HDFS
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_output],
                   check=False, capture_output=True)
    subprocess.run(["hdfs", "dfs", "-rm", "-f", hdfs_output + "/*"],
                   check=False, capture_output=True)
    result = subprocess.run(
        ["hdfs", "dfs", "-put", tmpfile, hdfs_output + "part-00000"],
        capture_output=True, text=True
    )
    os.unlink(tmpfile)

    if result.returncode == 0:
        print(f"  ✓ 已写入 {hdfs_output}")
    else:
        print(f"  ✗ 写入失败: {result.stderr}")

    # Top 10
    print("\n── 共现 TOP 10 ──")
    for i, ((a, b), c) in enumerate(
            sorted(pair_counts.items(), key=lambda x: -x[1])[:10], 1):
        print(f"  {i:2d}. {a} ↔ {b}: {c} 次")

    print("\n" + "=" * 60)
    print("DWS 配队共现计算完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
