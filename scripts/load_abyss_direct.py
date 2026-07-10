#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 深渊数据直接入库（从 v59 真实统计文件）
不使用生成器，直接用文件中 100 支队伍的使用率/持有率/头像 → MySQL
"""
import json, sys, time
from collections import defaultdict
try: import pymysql
except ImportError: print("[ERROR] pip install pymysql", file=sys.stderr); sys.exit(1)

DB = {"host": "Middleware", "port": 3306, "user": "root", "password": "123456",
      "database": "abyss_db", "charset": "utf8mb4"}

def main():
    json_path = sys.argv[1] if len(sys.argv) > 1 else "提瓦特数据/深渊配队汇总.json"
    with open(json_path, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)

    teams_raw = data if isinstance(data, list) else data.get("teams", [])
    chars_raw = data.get("chars", []) if isinstance(data, dict) else []

    if not teams_raw:
        print("[ERROR] 无配队数据", file=sys.stderr); return

    conn = pymysql.connect(**DB)
    c = conn.cursor()

    # 1. 检查缺失角色
    existing = set()
    c.execute("SELECT char_name FROM dim_role")
    for row in c.fetchall(): existing.add(row[0])

    missing = set()
    all_names = set()
    for t in teams_raw:
        for m in t.get("members", []):
            name = m.get("name", "?")
            all_names.add(name)
            if name not in existing: missing.add(name)
    if missing:
        print(f"[WARN] dim_role 缺失 {len(missing)} 个角色: {', '.join(sorted(missing))}", file=sys.stderr)

    # 2. 写配队：直接用文件里的 use_rate/has_rate
    c.execute("DELETE FROM ads_team_usage WHERE version_name='v6.6'")

    team_sql = """INSERT INTO ads_team_usage
        (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
        VALUES (%s,%s,%s,%s,%s,%s)"""
    team_rows = 0
    for t in teams_raw:
        members = t.get("members", [])
        names = [m.get("name", "?") for m in members]
        avatars = [m.get("avatar", "") for m in members]
        team_name = "+".join(names)
        c.execute(team_sql, ("v6.6", team_name, json.dumps(names, ensure_ascii=False),
                   json.dumps(avatars, ensure_ascii=False), t.get("use_rate", 0), t.get("has_rate", 0)))
        team_rows += 1

    # 3. 写角色：聚合所有配队中每个角色的使用次数（加权）
    c.execute("DELETE FROM ads_char_summary WHERE version_name='v6.6'")
    char_sql = """INSERT INTO ads_char_summary
        (version_name, char_name, star, use_rate, own_rate, avg_constellation, avg_level)
        VALUES (%s,%s,%s,%s,%s,1.0,90)"""

    char_use = defaultdict(float)
    char_own = defaultdict(float)
    char_star = {}
    for t in teams_raw:
        use = t.get("use_rate", 0)
        has = t.get("has_rate", 0)
        for m in t.get("members", []):
            name = m.get("name", "?")
            char_use[name] += use
            char_own[name] += has
            char_star[name] = m.get("star", 4)

    char_rows = 0
    for name in char_star:
        c.execute(char_sql, ("v6.6", name, char_star[name],
                   round(min(char_use[name], 100), 1), round(min(char_own[name], 100), 1)))
        char_rows += 1

    conn.commit(); c.close(); conn.close()

    top_teams = sorted(teams_raw, key=lambda t: t.get("use_rate", 0), reverse=True)[:5]
    print(f"[Abyss-Direct] ✓ {team_rows} 配队 + {char_rows} 角色写入完成", file=sys.stderr)
    print(f"[Abyss-Direct] TOP5 配队: {', '.join('+'.join(m.get('name','?') for m in t['members'])+'('+str(t['use_rate'])+'%)' for t in top_teams)}", file=sys.stderr)

if __name__ == "__main__":
    main()
