#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — DWS 层 抽卡数据聚合
按物品聚合五星出货统计。
下游: ADS 层 load_gacha_to_mysql.py
"""
import json, sys
from collections import defaultdict

def aggregate_gacha_results(five_star_records, total_pulls):
    agg = defaultdict(lambda: {"five_count": 0, "is_up_count": 0})
    for r in five_star_records:
        if agg[r["item"]]["five_count"] == 0:
            agg[r["item"]]["item"] = r["item"]
            agg[r["item"]]["type"] = r.get("type", "role")
        agg[r["item"]]["five_count"] += 1
        if r.get("is_up"): agg[r["item"]]["is_up_count"] += 1
    return {"total_pulls": total_pulls, "five_total": len(five_star_records),
            "five_rate": round(len(five_star_records)/total_pulls*100,2) if total_pulls>0 else 0,
            "items": dict(agg)}

def main():
    lines = [line.strip() for line in sys.stdin if line.strip()]
    records = [json.loads(line) for line in lines]
    total = int(sys.argv[1]) if len(sys.argv)>1 else len(records)
    agg = aggregate_gacha_results(records, total)
    print(json.dumps(agg, ensure_ascii=False))
    print(f"[DWS] 聚合 {len(agg['items'])} 物品, 五星率 {agg['five_rate']}%", file=sys.stderr)

if __name__ == "__main__":
    main()
