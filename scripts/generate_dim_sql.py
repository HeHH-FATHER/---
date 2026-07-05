#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 JSON 生成维表 INSERT SQL → Navicat 里粘贴执行
用法: python generate_dim_sql.py
输出: scripts/dim_data.sql
"""
import json
import os
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "提瓦特数据")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dim_data.sql")

lines = []
lines.append("USE abyss_db;")
lines.append("")

# ===== 1. dim_role =====
print("[1/3] dim_role...")
src = os.path.join(DATA_DIR, "角色列表_clean.json")
with open(src, "r", encoding="utf-8") as f:
    data = json.load(f)
lines.append("-- dim_role")
lines.append("TRUNCATE TABLE dim_role;")
for item in data:
    name = item["name"].replace("'", "''")
    avatar = item.get("avatar", "").replace("'", "''")
    lines.append(f"INSERT INTO dim_role (char_name, star, avatar) VALUES ('{name}', {item['star']}, '{avatar}');")
print(f"  {len(data)} 条")

# ===== 2. dim_weapon =====
print("[2/3] dim_weapon...")
src = os.path.join(DATA_DIR, "角色练度统计.json")
with open(src, "r", encoding="utf-8") as f:
    data = json.load(f)
weapons = {}
for char in data:
    for w in char.get("weapons", []):
        name = w.get("name", "").strip()
        avatar = w.get("avatar", "").strip()
        if name and name not in weapons:
            weapons[name] = avatar

lines.append("")
lines.append("-- dim_weapon")
lines.append("TRUNCATE TABLE dim_weapon;")
for name, avatar in weapons.items():
    safe_name = name.replace("'", "''")
    safe_avatar = avatar.replace("'", "''")
    lines.append(f"INSERT INTO dim_weapon (weapon_name, avatar) VALUES ('{safe_name}', '{safe_avatar}');")
print(f"  {len(weapons)} 把")

# ===== 3. dim_banner =====
print("[3/3] dim_banner...")
src = os.path.join(DATA_DIR, "卡池统计_clean.json")
with open(src, "r", encoding="utf-8") as f:
    data = json.load(f)
banners = data.get("roleList", [])

lines.append("")
lines.append("-- dim_banner")
lines.append("TRUNCATE TABLE dim_banner;")
count = 0
for b in banners:
    version = b.get("version", "").replace("'", "''")
    start = b.get("start_time", "") or "NULL"
    end = b.get("end_time", "") or "NULL"
    if start != "NULL":
        start = f"'{start}'"
    if end != "NULL":
        end = f"'{end}'"
    for char_name, pull_count in b.get("content", {}).items():
        safe_name = char_name.replace("'", "''")
        lines.append(f"INSERT INTO dim_banner (version_name, start_time, end_time, char_name, pull_count) VALUES ('{version}', {start}, {end}, '{safe_name}', {pull_count});")
        count += 1
print(f"  {count} 条")

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"\n[OK] 已生成: {OUTPUT}")
print(f"      大小: {os.path.getsize(OUTPUT):,} bytes")
print(f"      Navicat 中打开该文件 → 运行")
