#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — ADS 层 深渊数据聚合 + MySQL 写入
从 stdin 读取 preprocess_abyss.py 生成的 JSONL → ads_char_summary + ads_team_usage
"""
import json, sys, time
from collections import defaultdict
try: import pymysql
except ImportError: print("[ERROR] pip install pymysql", file=sys.stderr); sys.exit(1)

DB = {"host": "Middleware", "port": 3306, "user": "root", "password": "123456",
      "database": "abyss_db", "charset": "utf8mb4"}

def main():
    records = []
    for line in sys.stdin:
        line = line.strip()
        if not line: continue
        try: records.append(json.loads(line))
        except: pass

    if not records: return
    total = len(records)
    print(f"[Abyss-ADS] 读取 {total} 条玩家记录", file=sys.stderr)

    char_use = defaultdict(int); char_own = defaultdict(int)
    char_const = defaultdict(float); char_star = {}
    team_count = defaultdict(int); team_roles = {}

    for r in records:
        box = r.get("box", {})
        box_chars = box.get("characters", []) if isinstance(box, dict) else box
        for c in box_chars:
            char_own[c["name"]] += 1
            char_const[c["name"]] += c.get("constellation", 0)
            char_star[c["name"]] = c.get("star", 4)

        record = r.get("record") or {}
        rec_teams = record.get("teams", []) if isinstance(record, dict) else []
        team_chars = set()
        for t in rec_teams:
            members_raw = t.get("members", [])
            member_names = [m.get("name", m) if isinstance(m, dict) else str(m) for m in members_raw]
            key = "+".join(member_names) if member_names else "?"
            team_count[key] += 1
            if key not in team_roles:
                team_roles[key] = member_names
            for m in member_names:
                team_chars.add(m)
        for name in team_chars:
            char_use[name] += 1

    conn = pymysql.connect(**DB)
    c = conn.cursor()
    avatar_map = {}
    try:
        c.execute("SELECT char_name, avatar FROM dim_role")
        for row in c.fetchall():
            avatar_map[row[0]] = row[1] or ""
    except: pass

    version = "v6.6"
    c.execute("DELETE FROM ads_char_summary WHERE version_name=%s", (version,))
    c.execute("DELETE FROM ads_team_usage WHERE version_name=%s", (version,))

    char_rows = 0
    for name in char_own:
        use_rate = round(char_use[name] / total * 100, 2)
        own_rate = round(char_own[name] / total * 100, 2)
        avg_const = round(char_const[name] / max(char_own[name], 1), 1)
        c.execute("INSERT INTO ads_char_summary (version_name,char_name,star,use_rate,own_rate,avg_constellation,avg_level) VALUES (%s,%s,%s,%s,%s,%s,90)",
                  (version, name, char_star.get(name, 4), use_rate, own_rate, avg_const))
        char_rows += 1

    # 预计算每个玩家的 box（角色名集合）
    player_boxes = []
    for r in records:
        box = r.get("box", {})
        box_chars = box.get("characters", []) if isinstance(box, dict) else box
        player_boxes.append({c["name"] for c in box_chars})

    team_rows = 0
    for name in team_count:
        roles = team_roles.get(name, [])
        # 持有率 = 拥有这四个角色的玩家数 / 总玩家数
        own_all = sum(1 for box in player_boxes if all(r in box for r in roles))
        has_rate = round(own_all / max(total, 1) * 100, 1)
        # 使用率 = 使用该队的玩家数 / 拥有这四个角色的玩家数
        use_rate = round(team_count[name] / max(own_all, 1) * 100, 1)
        avatars = [avatar_map.get(r, "") for r in roles]
        c.execute("INSERT INTO ads_team_usage (version_name,team_name,roles_json,avatars_json,use_rate,has_rate) VALUES (%s,%s,%s,%s,%s,%s)",
                  (version, name, json.dumps(roles, ensure_ascii=False), json.dumps(avatars, ensure_ascii=False), use_rate, has_rate))
        team_rows += 1

    conn.commit(); c.close(); conn.close()
    top = sorted(char_use.items(), key=lambda x: -x[1])[:10]
    print(f"[Abyss-ADS] MySQL写入完成: {char_rows} 角色 + {team_rows} 配队", file=sys.stderr)
    print(f"[Abyss-ADS] TOP10: {', '.join(f'{n}({c})' for n,c in top)}", file=sys.stderr)

if __name__ == "__main__":
    main()
