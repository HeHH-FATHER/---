#!/usr/bin/env python3
"""从生成器输出统计角色+配队 → MySQL（完全基于生成器，比例来自静态文件）"""
import json, os, sys, pymysql
from collections import defaultdict

GEN_DIR = sys.argv[1] if len(sys.argv) > 1 else "output/abyss/"
VERSION = sys.argv[2] if len(sys.argv) > 2 else "6.6深渊使用率统计(第一期)"
MYSQL_HOST = "100.103.177.85"

# ── 统计 ──
char_use = defaultdict(int)       # 角色 → 使用次数（上场）
char_own = defaultdict(int)       # 角色 → 拥有用户数
char_own_count = defaultdict(int) # 角色 → 拥有总次数
char_star = {}                    # 角色 → 星级
team_counts = defaultdict(int)    # 配队名 → 次数
team_avatars = {}                 # 配队名 → 头像列表
total_users = 0

# 预加载头像映射
conn0 = pymysql.connect(host=MYSQL_HOST, port=3306, user="root", password="123456",
                        database="abyss_db", charset="utf8mb4")
cur0 = conn0.cursor()
cur0.execute("SELECT char_name, avatar FROM dim_role")
avatar_map = {row[0]: row[1] for row in cur0.fetchall()}
cur0.close()
conn0.close()

for fn in os.listdir(GEN_DIR):
    if not fn.endswith("_abyss_record.json"):
        continue
    total_users += 1
    uid = fn.replace("_abyss_record.json", "")
    box_fn = uid + "_char_box.json"

    # 持有率：从 char_box.json
    try:
        with open(os.path.join(GEN_DIR, box_fn), encoding="utf-8") as f:
            box = json.load(f)
        for c in box.get("characters", []):
            name = c.get("name", "?")
            char_own[name] += 1
            char_own_count[name] += c.get("count", 1)
            if name not in char_star:
                char_star[name] = c.get("star", 4)
    except: pass

    # 使用率：从 abyss_record.json
    with open(os.path.join(GEN_DIR, fn), encoding="utf-8") as f:
        data = json.load(f)
    seen = set()
    for t in data.get("teams", []):
        members = t.get("members", [])
        for m in members:
            name = m.get("name", "?")
            if name not in char_star:
                char_star[name] = m.get("star", 4)
            if name not in seen:
                char_use[name] += 1
                seen.add(name)
        # 配队统计
        if len(members) >= 4:
            names = [m.get("name", "?") for m in members[:4]]
            key = "+".join(names)
            team_counts[key] += 1
            if key not in team_avatars:
                team_avatars[key] = [avatar_map.get(m.get("name",""), "") for m in members[:4]]

# ── 写入 MySQL ──
conn = pymysql.connect(host=MYSQL_HOST, port=3306, user="root", password="123456",
                       database="abyss_db", charset="utf8mb4")
cur = conn.cursor()

# 加载头像映射
cur.execute("SELECT char_name, avatar FROM dim_role")
avatar_map = {row[0]: row[1] for row in cur.fetchall()}

# 清空当前版本旧数据
cur.execute("DELETE FROM ads_char_summary WHERE version_name=%s", (VERSION,))
# 配队不删，用 ON DUPLICATE KEY 更新——保留静态数据，叠加生成器数据

# 写入角色（包含拥有但未上场的）
all_chars = set(list(char_own.keys()) + list(char_use.keys()))
for name in all_chars:
    star = char_star.get(name, 4)
    own_count = char_own.get(name, 0)
    use_count = char_use.get(name, 0)
    own_rate = round(own_count / total_users * 100, 1) if own_count > 0 else 0
    use_rate = round(use_count / total_users * 100, 1)
    cur.execute("""INSERT INTO ads_char_summary (version_name, char_name, star, own_count, use_count, total_users, own_rate, use_rate)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (VERSION, name, star, own_count, use_count, total_users, own_rate, use_rate))

# 写入配队（带头像）
for team_name, count in sorted(team_counts.items(), key=lambda x: -x[1]):
    use_rate = round(count / total_users * 100, 1)
    has_rate = round(count / total_users * 100 * 0.7, 1)
    roles_json = json.dumps(team_name.split(" + "), ensure_ascii=False)
    avatars_json = json.dumps(team_avatars.get(team_name, []), ensure_ascii=False)
    cur.execute("""INSERT INTO ads_team_usage (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE use_rate=VALUES(use_rate), has_rate=VALUES(has_rate), avatars_json=VALUES(avatars_json)""",
        (VERSION, team_name, roles_json, avatars_json, use_rate, has_rate))

conn.commit()
cur.close()
conn.close()

print(f"用户:{total_users} 角色:{len(char_use)} 配队:{len(team_counts)}")
print(f"TOP5角色: {', '.join([f'{n}({round(c/total_users*100,1)}%)' for n,c in sorted(char_use.items(),key=lambda x:-x[1])[:5]])}")
print(f"TOP3配队: {', '.join([f'{n}({round(c/total_users*100,1)}%)' for n,c in sorted(team_counts.items(),key=lambda x:-x[1])[:3]])}")
