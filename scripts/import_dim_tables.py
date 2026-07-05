#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 维表数据导入脚本
从提瓦特 JSON 提取角色/武器/卡池数据 → 写入 MySQL
用法: python import_dim_tables.py
"""

import json
import pymysql
import os
import sys

# 中文输出
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "提瓦特数据")

DB_CONFIG = {
    "host": "100.103.177.85",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "abyss_db",
    "charset": "utf8",
}


def import_roles(conn):
    """导入角色维表"""
    print("[1/3] 导入 dim_role...")
    src = os.path.join(DATA_DIR, "角色列表_clean.json")
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_role")
    count = 0
    for item in data:
        cursor.execute(
            "INSERT INTO dim_role (char_name, star, avatar) VALUES (%s, %s, %s)",
            (item["name"], item["star"], item.get("avatar", ""))
        )
        count += 1
    conn.commit()
    cursor.close()
    print(f"  [OK] 导入 {count} 条角色")


def import_weapons(conn):
    """从练度统计中提取所有武器→武器维表"""
    print("[2/3] 导入 dim_weapon...")
    src = os.path.join(DATA_DIR, "角色练度统计.json")
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    weapons = {}  # name -> avatar
    for char in data:
        for w in char.get("weapons", []):
            name = w.get("name", "").strip()
            avatar = w.get("avatar", "")
            if name and name not in weapons:
                weapons[name] = avatar

    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_weapon")
    count = 0
    for name, avatar in weapons.items():
        cursor.execute(
            "INSERT INTO dim_weapon (weapon_name, avatar) VALUES (%s, %s)",
            (name, avatar)
        )
        count += 1
    conn.commit()
    cursor.close()
    print(f"  [OK] 导入 {count} 把武器")


def import_banners(conn):
    """导入卡池历史维表"""
    print("[3/3] 导入 dim_banner...")
    src = os.path.join(DATA_DIR, "卡池统计_clean.json")
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    banners = data.get("roleList", [])

    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_banner")
    count = 0
    for b in banners:
        version = b.get("version", "")
        start = b.get("start_time", None)
        end = b.get("end_time", None)
        content = b.get("content", {})
        for char_name, pull_count in content.items():
            cursor.execute(
                "INSERT INTO dim_banner (version_name, start_time, end_time, char_name, pull_count) VALUES (%s, %s, %s, %s, %s)",
                (version, start, end, char_name, pull_count)
            )
            count += 1
    conn.commit()
    cursor.close()
    print(f"  [OK] 导入 {count} 条卡池记录 ({len(banners)} 期)")


def main():
    print("=" * 50)
    print("维表数据导入")
    print("=" * 50)

    try:
        conn = pymysql.connect(**DB_CONFIG)
        print(f"[OK] 连接 MySQL {DB_CONFIG['host']}:{DB_CONFIG['port']}\n")
    except Exception as e:
        print(f"[ERROR] 连接失败: {e}")
        print(f"提示: pip install pymysql")
        return

    try:
        import_roles(conn)
        import_weapons(conn)
        import_banners(conn)
        print("\n[OK] 三条维表全部导入完成")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
