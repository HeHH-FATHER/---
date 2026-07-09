#!/usr/bin/env python3
"""
一次性脚本：从角色练度统计.json 提取所有武器图标 → 灌入 MySQL dim_weapon 表
用法：python3 import_weapon_icons.py
"""
import json
import pymysql

# 读取练度统计
with open('提瓦特数据/角色练度统计.json', 'r', encoding='utf-8') as f:
    stats = json.load(f)

# 提取武器名→图标URL（去重）
weapons = {}
for char in stats:
    for w in char.get('weapons', []):
        name = w.get('name', '').strip()
        avatar = w.get('avatar', '').strip()
        if name and avatar and name not in weapons:
            weapons[name] = avatar

print(f"提取到 {len(weapons)} 把武器")

# 写入 MySQL
conn = pymysql.connect(host='100.103.177.85', port=3306, user='root', password='123456', database='abyss_db', charset='utf8mb4')
cursor = conn.cursor()

inserted = 0
updated = 0
for name, avatar in weapons.items():
    cursor.execute("SELECT id FROM dim_weapon WHERE weapon_name = %s", (name,))
    if cursor.fetchone():
        cursor.execute("UPDATE dim_weapon SET avatar = %s WHERE weapon_name = %s", (avatar, name))
        updated += 1
    else:
        cursor.execute("INSERT INTO dim_weapon (weapon_name, avatar) VALUES (%s, %s)", (name, avatar))
        inserted += 1

conn.commit()
cursor.close()
conn.close()
print(f"完成：新增 {inserted}，更新 {updated}")
