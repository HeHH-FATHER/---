#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 深渊数据直接入库（从 v59 真实统计文件）
不使用生成器，直接用文件中 100 支队伍的使用率/持有率/头像 → MySQL
"""
import json, sys, time
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

    # 2. 写配队：INSERT ON DUPLICATE KEY UPDATE（不删已有数据）
    team_sql = """INSERT INTO ads_team_usage
        (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE use_rate=VALUES(use_rate), has_rate=VALUES(has_rate),
        avatars_json=VALUES(avatars_json), roles_json=VALUES(roles_json)"""
    team_rows = 0
    for t in teams_raw:
        members = t.get("members", [])
        names = [m.get("name", "?") for m in members]
        avatars = [m.get("avatar", "") for m in members]
        team_name = "+".join(names)
        c.execute(team_sql, ("6.6深渊使用率统计(第一期)", team_name, json.dumps(names, ensure_ascii=False),
                   json.dumps(avatars, ensure_ascii=False), t.get("use_rate", 0), t.get("has_rate", 0)))
        team_rows += 1

    conn.commit(); c.close(); conn.close()

    top_teams = sorted(teams_raw, key=lambda t: t.get("use_rate", 0), reverse=True)[:5]
    print(f"[Abyss-Direct] ✓ {team_rows} 配队写入完成", file=sys.stderr)
    print(f"[Abyss-Direct] TOP5 配队: {', '.join('+'.join(m.get('name','?') for m in t['members'])+'('+str(t['use_rate'])+'%)' for t in top_teams)}", file=sys.stderr)

if __name__ == "__main__":
    main()
