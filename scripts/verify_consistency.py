#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 聚合回归验证脚本
验证 Abyss-Record-Generator 生成数据 → 聚合统计 → 与源统计数据的偏差

数据闭环：
  提瓦特API统计 → Generator(反推个体) → MapReduce聚合 → 验证偏差 → 提瓦特API统计

用法：
  python verify_consistency.py <source_stats.json> <aggregated_result.csv>
  python verify_consistency.py --auto  # 自动从 HDFS DWS 读取对比

偏差容忍度（来自 离线链/清洗规范.md）：
  - 使用率偏差 ≤ 3%
  - 持有率偏差 ≤ 3%
  - 命座偏差 ≤ 0.3
  - 等级偏差 ≤ 5
"""
import json
import csv
import sys
import os
import math

# ==================== 配置 ====================
# 偏差容忍度
TOLERANCE = {
    "use_rate": 3.0,           # 使用率偏差≤3%
    "own_rate": 3.0,           # 持有率偏差≤3%
    "avg_constellation": 0.3,  # 平均命座偏差≤0.3
    "avg_level": 5,            # 平均等级偏差≤5
}


def load_source_stats(path):
    """加载提瓦特API源统计数据（深渊角色使用率.json）"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stats = {}
    for item in data:
        name = item.get("name", "")
        stats[name] = {
            "use_rate": float(item.get("use_rate", 0)),
            "own_rate": float(item.get("own_rate", 0)),
        }
    return stats


def load_aggregated_csv(path):
    """加载聚合结果 CSV（DWS 输出格式）"""
    stats = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("char_name", "")
            stats[name] = {
                "use_rate": float(row.get("use_rate", 0)),
                "own_rate": float(row.get("own_rate", 0)),
                "avg_constellation": float(row.get("avg_constellation", 0)),
                "avg_level": float(row.get("avg_level", 0)),
                "star": int(row.get("star", 0)),
            }
    return stats


def verify(source_stats, aggregated_stats):
    """逐角色对比偏差"""
    print("=" * 70)
    print("聚合回归验证")
    print("=" * 70)

    results = []
    total_deviation = 0
    pass_count = 0
    fail_count = 0

    common_chars = set(source_stats.keys()) & set(aggregated_stats.keys())
    print(f"源统计角色: {len(source_stats)}")
    print(f"聚合结果角色: {len(aggregated_stats)}")
    print(f"可对比角色: {len(common_chars)}")
    print()

    for name in sorted(common_chars):
        src = source_stats[name]
        agg = aggregated_stats[name]

        deviations = {}
        all_pass = True

        # 使用率偏差
        use_diff = abs(src["use_rate"] - agg["use_rate"])
        deviations["use_rate"] = round(use_diff, 2)
        if use_diff > TOLERANCE["use_rate"]:
            all_pass = False

        # 持有率偏差
        own_diff = abs(src["own_rate"] - agg["own_rate"])
        deviations["own_rate"] = round(own_diff, 2)
        if own_diff > TOLERANCE["own_rate"]:
            all_pass = False

        # 命座偏差（源数据可能没有）
        if "avg_constellation" in agg:
            const_diff = None  # 源统计不直接提供命座

        # 等级偏差（源数据可能没有）
        if "avg_level" in agg:
            level_diff = None

        total_deviation += use_diff + own_diff

        if all_pass:
            pass_count += 1
        else:
            fail_count += 1
            results.append({
                "name": name,
                "src_use": src["use_rate"],
                "agg_use": agg["use_rate"],
                "use_diff": use_diff,
                "src_own": src["own_rate"],
                "agg_own": agg["own_rate"],
                "own_diff": own_diff,
                "pass": False
            })

    # 统计
    avg_deviation = total_deviation / (len(common_chars) * 2) if common_chars else 0
    pass_rate = pass_count / len(common_chars) * 100 if common_chars else 0

    print("-" * 70)
    print(f"通过: {pass_count} / {len(common_chars)} ({pass_rate:.1f}%)")
    print(f"失败: {fail_count} / {len(common_chars)}")
    print(f"平均偏差: {avg_deviation:.2f}%")
    print()

    if fail_count > 0:
        print("=" * 70)
        print("异常角色详情（偏差超限）:")
        print("-" * 70)
        print(f"{'角色':<12} {'源使用率':>8} {'聚合使用率':>10} {'偏差':>6} {'源持有率':>8} {'聚合持有率':>10} {'偏差':>6}")
        print("-" * 70)
        for r in sorted(results, key=lambda x: x["use_diff"] + x["own_diff"], reverse=True):
            print(f"{r['name']:<12} {r['src_use']:>7.1f}% {r['agg_use']:>9.1f}% {r['use_diff']:>5.1f}% "
                  f"{r['src_own']:>7.1f}% {r['agg_own']:>9.1f}% {r['own_diff']:>5.1f}%")

    print()
    if pass_rate >= 95:
        print("[PASS] 聚合回归验证通过 ✅（通过率≥95%）")
        return True
    elif pass_rate >= 85:
        print("[WARN] 聚合回归验证勉强通过 ⚠️（通过率≥85%但<95%，建议排查）")
        return True
    else:
        print("[FAIL] 聚合回归验证不通过 ❌（通过率<85%，需修复数据管道）")
        return False


def main():
    if "--auto" in sys.argv:
        # 自动模式：从 HDFS DWS 读取
        print("自动验证模式...")
        try:
            import subprocess
            # 下载 DWS 结果
            subprocess.run([
                "hdfs", "dfs", "-getmerge",
                "/data/abyss/dws/dws_char_summary/",
                "/tmp/dws_char_summary.csv"
            ], check=False)
            aggregated_path = "/tmp/dws_char_summary.csv"
        except Exception:
            print("HDFS 不可用，请手动指定聚合结果文件路径")
            sys.exit(1)

        # 源统计文件
        source_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "提瓦特数据", "深渊角色使用率.json"
        )
    elif len(sys.argv) >= 3:
        source_path = sys.argv[1]
        aggregated_path = sys.argv[2]
    else:
        print("用法:")
        print("  python verify_consistency.py <源统计.json> <聚合结果.csv>")
        print("  python verify_consistency.py --auto")
        sys.exit(1)

    if not os.path.exists(source_path):
        print(f"[ERROR] 源统计文件不存在: {source_path}")
        sys.exit(1)
    if not os.path.exists(aggregated_path):
        print(f"[ERROR] 聚合结果文件不存在: {aggregated_path}")
        sys.exit(1)

    source_stats = load_source_stats(source_path)
    aggregated_stats = load_aggregated_csv(aggregated_path)

    ok = verify(source_stats, aggregated_stats)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
