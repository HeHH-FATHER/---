#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — DWD 层 抽卡数据清洗
过滤非五星记录，仅保留 5★ 出货。
下游: DWS 层 aggregate_gacha.py
"""
import json, sys

def filter_five_star_records(records):
    return [r for r in records if r.get("star") == 5]

def main():
    lines = [line.strip() for line in sys.stdin if line.strip()]
    records = []
    for line in lines:
        try: records.append(json.loads(line))
        except json.JSONDecodeError: pass
    total = len(records)
    five = filter_five_star_records(records)
    print(f"[DWD] {total} 抽 → {len(five)} 五星 (五星率 {len(five)/total*100:.2f}%)", file=sys.stderr)
    for r in five:
        print(json.dumps(r, ensure_ascii=False))

if __name__ == "__main__":
    main()
