#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 实时链 Producer 单元测试
测试 build_producer.py 和 gacha_producer.py 的输出格式
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data_generator"))

PASS = 0
FAIL = 0
def chk(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  [PASS] {name}")
    else: FAIL += 1; print(f"  [FAIL] {name} — {detail}")
    return cond

print("=" * 60)
print("  实时链 Producer 单元测试")
print("=" * 60)

# ============================================================
# 1. build_producer.py
# ============================================================
print("\n--- build_producer.py ---")

from build_producer import load_data, pick_constellation, pick_from_rate, generate_record

chars = load_data()
chk("加载练度数据", len(chars) > 0, f"实际: {len(chars)} 个角色")

# 测试单条记录生成
rec = generate_record(100000001, chars)
chk("记录含 uid", "uid" in rec)
chk("记录含 char", "char" in rec)
chk("记录含 constellation", "constellation" in rec and 0 <= rec["constellation"] <= 6,
    f"值: {rec.get('constellation')}")
chk("记录含 weapon", "weapon" in rec and len(rec["weapon"]) > 0,
    f"值: {rec.get('weapon')}")
chk("记录含 artifact", "artifact" in rec and len(rec["artifact"]) > 0,
    f"值: {rec.get('artifact')}")
chk("记录含 damage", "damage" in rec and rec["damage"] > 0,
    f"值: {rec.get('damage')}")
chk("记录含 timestamp", "timestamp" in rec and rec["timestamp"] > 0)
chk("star 合法", rec["star"] in (4, 5), f"值: {rec.get('star')}")
chk("level 合法", rec["level"] in (80, 90), f"值: {rec.get('level')}")

# 测试命座分布: 1000 次都在 0~6
all_ok = all(0 <= pick_constellation(chars[0]["const_dist"]) <= 6 for _ in range(1000))
chk("命座分布 0~6", all_ok)

# 测试武器选择: 100 次都有值
weapons = [pick_from_rate(chars[0]["weapons"]) for _ in range(100)]
chk("武器选择非空", all(w for w in weapons))

# 测试圣遗物选择
artifacts = [pick_from_rate(chars[0]["artifacts"]) for _ in range(100)]
chk("圣遗物选择非空", all(a for a in artifacts))

# ============================================================
# 2. gacha_producer.py
# ============================================================
print("\n--- gacha_producer.py ---")

import gacha_common
banners = gacha_common.load_banners()
chk("加载卡池数据", len(banners) > 0, f"实际: {len(banners)} 期")

# 验证卡池字段
b = banners[0]
chk("卡池含 version", "version" in b, f"实际字段: {list(b.keys())}")
chk("卡池含 total", "total" in b and b["total"] > 0)
chk("卡池含 weight", "weight" in b and b["weight"] > 0)
chk("卡池含 chars", "chars" in b and len(b["chars"]) > 0)

# 测试 pick_char（需要完整 banner dict）
char = gacha_common.pick_char(b)
chk("pick_char 返回值", isinstance(char, str) and len(char) > 0)

# 测试 generate_record
pity = {}
rec, pity = gacha_common.generate_record(banners, pity)
chk("抽卡记录含 uid", "uid" in rec)
chk("抽卡记录含 item", "item" in rec)
chk("抽卡记录含 star", "star" in rec and rec["star"] in (3, 4, 5))
chk("抽卡记录含 banner", "banner" in rec)

# ============================================================
# 3. 产出文件验证
# ============================================================
print("\n--- 产出文件验证 ---")

# build_records.json
build_path = os.path.join(os.path.dirname(__file__), "..", "data_generator", "output", "build_records.json")
if os.path.exists(build_path):
    with open(build_path, "r", encoding="utf-8") as f:
        build_data = json.load(f)
    chk("build_records.json 非空", len(build_data) > 0, f"共 {len(build_data)} 条")
    chk("每条都是 dict", all(isinstance(d, dict) for d in build_data))
    chk("每条都有 9 个字段", all(len(d) == 9 for d in build_data))
else:
    chk("build_records.json 存在", False, "文件不存在，先跑 python build_producer.py")

# gacha_records.csv
gacha_path = os.path.join(os.path.dirname(__file__), "..", "data_generator", "output", "gacha_records.csv")
if os.path.exists(gacha_path):
    with open(gacha_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()
    chk("gacha_records.csv 有数据", len(lines) > 1, f"共 {len(lines)} 行(含表头)")
    chk("第一行含 uid", lines[0].startswith("uid"))
    chk("每行 6 列(逗号分隔)", all(len(l.strip().split(",")) == 6 for l in lines[1:11]))
else:
    chk("gacha_records.csv 存在", False, "文件不存在，先跑 python gacha_producer.py")

# ============================================================
print(f"\n{'=' * 60}")
print(f"  通过: {PASS}/{PASS+FAIL}")
if FAIL > 0:
    print(f"  [FAIL] {FAIL} 项未通过")
else:
    print(f"  [PASS] 全部通过!")
