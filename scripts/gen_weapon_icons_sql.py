#!/usr/bin/env python3
"""提取角色练度统计.json 中的武器图标 → 生成 SQL"""
import json

with open('提瓦特数据/角色练度统计.json', 'r', encoding='utf-8') as f:
    stats = json.load(f)

weapons = {}
for char in stats:
    for w in char.get('weapons', []):
        name = w.get('name', '').strip()
        avatar = w.get('avatar', '').strip()
        if name and avatar and name not in weapons:
            weapons[name] = avatar

lines = []
for name, avatar in weapons.items():
    safe_name = name.replace("'", "''")
    safe_avatar = avatar.replace("'", "''")
    lines.append(f"INSERT INTO dim_weapon (weapon_name, avatar) VALUES ('{safe_name}', '{safe_avatar}') ON DUPLICATE KEY UPDATE avatar = VALUES(avatar);")

with open('scripts/weapon_icons.sql', 'w', encoding='utf-8') as f:
    f.write("-- 武器图标数据（来源：提瓦特数据/角色练度统计.json）\n")
    f.write(f"-- 共 {len(lines)} 条\n\n")
    f.write('\n'.join(lines))
    f.write('\n')

print(f"生成 scripts/weapon_icons.sql，共 {len(lines)} 条")
