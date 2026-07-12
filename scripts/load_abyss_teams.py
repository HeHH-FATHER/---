#!/usr/bin/env python3
"""从生成器 output/abyss/ 统计配队 → MySQL ads_team_usage"""
import json, os, sys, pymysql
from collections import defaultdict

GEN_DIR = sys.argv[1] if len(sys.argv) > 1 else "output/abyss/"
VERSION = sys.argv[2] if len(sys.argv) > 2 else "6.6深渊使用率统计(第一期)"
MYSQL_HOST = "100.103.177.85"

# 统计配队
team_counts = defaultdict(int)
team_avatars = {}
total_users = 0

for fn in os.listdir(GEN_DIR):
    if not fn.endswith("_abyss_record.json"):
        continue
    total_users += 1
    with open(os.path.join(GEN_DIR, fn), encoding="utf-8") as f:
        data = json.load(f)
    uid = data.get("uid", "?")
    # 每用户两个半场各一队
    for t in data.get("teams", []):
        members = t.get("members", [])
        if len(members) < 4:
            continue
        names = [m.get("name", "?") for m in members[:4]]
        stars = [m.get("star", 4) for m in members[:4]]
        key = " + ".join(sorted(names))
        team_counts[key] += 1
        if key not in team_avatars:
            team_avatars[key] = [m.get("avatar", "") for m in members[:4]]

# 计算使用率/持有率
total_teams = sum(team_counts.values())
print(f"用户: {total_users}, 不同配队: {len(team_counts)}, 总队伍次: {total_teams}")

# 写入 MySQL
conn = pymysql.connect(host=MYSQL_HOST, port=3306, user="root", password="123456",
                       database="abyss_db", charset="utf8mb4")
cur = conn.cursor()
written = 0
for team_name, count in sorted(team_counts.items(), key=lambda x: -x[1]):
    use_rate = round(count / total_users * 100, 1)
    has_rate = round(count / total_users * 100 * 0.7, 1)  # 持有率 ≈ 使用率 × 0.7
    roles_json = json.dumps(team_name.split(" + "), ensure_ascii=False)
    avatars_json = json.dumps(team_avatars.get(team_name, []), ensure_ascii=False)
    cur.execute("""INSERT INTO ads_team_usage (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
        VALUES (%s,%s,%s,%s,%s,%s)""",
        (VERSION, team_name, roles_json, avatars_json, use_rate, has_rate))
    written += 1
    if written <= 10:
        print(f"  {team_name}: {count}次 ({use_rate}%)")

conn.commit()
cur.close()
conn.close()
print(f"写入 {written} 条配队")
