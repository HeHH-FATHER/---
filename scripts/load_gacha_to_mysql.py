#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — ADS 层 抽卡数据加载
═══════════════════════════════════════════════════════════════
功能: 从 ODS 逐抽记录中过滤五星 → 聚合 → 写入 MySQL。
      包含 DWD(过滤) + DWS(聚合) + ADS(写库) 三层逻辑。

层级: DWD→DWS→ADS（MySQL）— 大屏/数据质量消费
上游: ODS 层 preprocess_gacha.py 输出的逐抽 JSON

加载目标:
  ads_gacha_offline — 每批次按物品聚合的五星出货统计

用法:
  python3 preprocess_gacha.py | python3 load_gacha_ads.py          # 管道串联
  python3 load_gacha_ads.py < raw_pulls.jsonl                      # 从文件读
  python3 load_gacha_ads.py --input raw_pulls.jsonl                # 指定输入
═══════════════════════════════════════════════════════════════════
"""
import json, sys, os, time
from collections import defaultdict

HOST = os.environ.get("MYSQL_HOST", "100.103.177.85")
USER = os.environ.get("MYSQL_USER", "root")
PASS = os.environ.get("MYSQL_PASS", "123456")
DB   = os.environ.get("MYSQL_DB", "abyss_db")

try: import pymysql
except ImportError: print("[FATAL] 需要 pymysql: pip install pymysql"); sys.exit(1)


# ═══════════════════════════════════════════════
# DWD 层: 过滤非五星
# ═══════════════════════════════════════════════

def filter_five_star_records(records):
    """筛选仅保留五星出货记录"""
    return [r for r in records if r.get("star") == 5]


# ═══════════════════════════════════════════════
# DWS 层: 按物品聚合
# ═══════════════════════════════════════════════

def aggregate_gacha_results(five_star_records, total_pulls):
    """按物品聚合五星出货统计"""
    agg = defaultdict(lambda: {"five_count": 0, "is_up_count": 0})
    for r in five_star_records:
        agg[r["item"]]["five_count"] += 1
        if r.get("is_up"):
            agg[r["item"]]["is_up_count"] += 1
    return {
        "total_pulls": total_pulls,
        "five_total": len(five_star_records),
        "five_rate": round(len(five_star_records) / total_pulls * 100, 2) if total_pulls > 0 else 0,
        "items": dict(agg)
    }


# ═══════════════════════════════════════════════
# ADS 层: 写入 MySQL
# ═══════════════════════════════════════════════

def load_gacha_to_mysql(aggregated, batch_time):
    """批量写入 ads_gacha_offline 表"""
    conn = pymysql.connect(host=HOST, user=USER, password=PASS, database=DB, charset="utf8mb4")
    c = conn.cursor()

    # 清理 7 天前数据
    c.execute("DELETE FROM ads_gacha_offline WHERE batch_time < NOW() - INTERVAL 7 DAY")
    sql = """INSERT INTO ads_gacha_offline (batch_time, item_name, item_type, five_star_count, up_count, five_rate, total_pulls, pity_triggered)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    total_up = sum(d["is_up_count"] for d in aggregated["items"].values())
    count = 0
    for key, data in aggregated["items"].items():
        item_name = data.get("item", key)
        item_type = data.get("type", "role")
        c.execute(sql, (batch_time, item_name, item_type, data["five_count"], data["is_up_count"], aggregated["five_rate"], aggregated["total_pulls"], 0))
        count += 1
    # 汇总行
    c.execute(sql, (batch_time, "__ALL__", "summary", aggregated["five_total"], total_up, aggregated["five_rate"], aggregated["total_pulls"], 0))
    conn.commit()
    c.close()
    conn.close()
    return count + 1


# ═══════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════

def main():
    input_file = None
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--input" and i+1 < len(args):
            input_file = args[i+1]

    print("=" * 60)
    print("逐风数据洞察平台 — ADS 层 抽卡数据加载")

    # 读取输入
    if input_file:
        with open(input_file, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    else:
        lines = [line.strip() for line in sys.stdin if line.strip()]

    # 尝试解析：单个 JSON 对象（上游已聚合）或 JSON 行（逐抽记录）
    agg = None
    if len(lines) == 1:
        try:
            obj = json.loads(lines[0])
            if isinstance(obj, dict) and "items" in obj:
                agg = obj  # 上游已聚合
        except json.JSONDecodeError: pass

    if agg is None:
        records = []
        for line in lines:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError: continue
        if not records:
            print("[ERROR] 无有效输入数据")
            sys.exit(1)

        print(f"[ODS] 读取 {len(records)} 条逐抽记录")
        five_stars = filter_five_star_records(records)
        five_rate = round(len(five_stars) / len(records) * 100, 2)
        print(f"[DWD] 过滤 → {len(five_stars)} 条五星 (五星率 {five_rate}%)")
        agg = aggregate_gacha_results(five_stars, len(records))
        print(f"[DWS] 聚合 → {len(agg['items'])} 个物品")
    else:
        print(f"[ODS-DWS] 上游已聚合 → {len(agg['items'])} 个物品, 五星率 {agg['five_rate']}%")

    # ADS
    batch_time = time.strftime("%Y-%m-%d %H:%M:%S")
    count = load_gacha_to_mysql(agg, batch_time)
    print(f"[ADS] 写入 {count} 行 → ads_gacha_offline")
    print(f"[OK] 批次 {batch_time} 完成")
    print(f"  五星率: {agg['five_rate']}% | 物品数: {len(agg['items'])} | TOP: {sorted(agg['items'].items(), key=lambda x:-x[1]['five_count'])[:3]}")


if __name__ == "__main__":
    main()
