#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 深渊数据融合入库
基底: v59 文件 (79000+样本, weight=79) + 生成器产出 (200玩家, weight=2)
融合公式: result = (base * base_wt + generated * gen_wt) / (base_wt + gen_wt)
"""
import json, sys, time
from collections import defaultdict
try: import pymysql
except ImportError: print("[ERROR] pip install pymysql", file=sys.stderr); sys.exit(1)

DB = {"host": "Middleware", "port": 3306, "user": "root", "password": "123456",
      "database": "abyss_db", "charset": "utf8mb4"}

BASE_FILE = "提瓦特数据/深渊配队汇总.json"
GEN_FILE = "/tmp/abyss_v66.jsonl"
BASE_WEIGHT = 79   # 基底 ~79000 样本
GEN_WEIGHT = 2     # 生成器 200 玩家

def load_base():
    """从 v59 文件加载基底数据"""
    with open(BASE_FILE, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)
    teams_raw = data if isinstance(data, list) else data.get("teams", [])

    char_use = defaultdict(float); char_own = defaultdict(float)
    char_star = {}
    teams = []  # [(name, roles, avatars, use_rate, has_rate), ...]

    for t in teams_raw:
        members = t.get("members", [])
        names = [m.get("name", "?") for m in members]
        avatars = [m.get("avatar", "") for m in members]
        use_r = t.get("use_rate", 0)
        has_r = t.get("has_rate", 0)
        teams.append(("+".join(names), names, avatars, use_r, has_r))

        for m in members:
            name = m.get("name", "?")
            char_use[name] += use_r
            char_own[name] += has_r
            char_star[name] = m.get("star", 4)

    return teams, dict(char_use), dict(char_own), char_star

def load_generated():
    """从生成器 JSONL 加载新数据"""
    teams = defaultdict(int)  # key → count
    team_roles = {}
    char_use = defaultdict(int)
    char_own = defaultdict(int)
    char_const = defaultdict(float)
    char_star = {}
    total = 0

    try:
        with open(GEN_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try: r = json.loads(line)
                except: continue
                total += 1

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
                    names = [m.get("name", m) if isinstance(m, dict) else str(m) for m in members_raw]
                    key = "+".join(names) if names else "?"
                    teams[key] += 1
                    if key not in team_roles:
                        team_roles[key] = names
                    for m in names: team_chars.add(m)
                for name in team_chars:
                    char_use[name] += 1
    except: pass

    # 转比率
    team_list = []
    team_total = sum(teams.values())
    for key, cnt in teams.items():
        team_list.append((key, team_roles.get(key, []), [], round(cnt/max(team_total,1)*100, 1), round(cnt/max(total,1)*100, 1)))

    char_use_r = {k: v/max(total,1)*100 for k, v in char_use.items()}
    char_own_r = {k: v/max(total,1)*100 for k, v in char_own.items()}
    return team_list, char_use_r, char_own_r, char_star

def weighted_merge(base_val, gen_val, base_wt=BASE_WEIGHT, gen_wt=GEN_WEIGHT):
    """加权融合，基底主导"""
    if base_val == 0: return gen_val
    if gen_val == 0: return base_val
    return round((base_val * base_wt + gen_val * gen_wt) / (base_wt + gen_wt), 1)

def main():
    # 加载基底
    base_teams, base_char_use, base_char_own, base_char_star = load_base()
    # 加载生成器产出
    gen_teams, gen_char_use, gen_char_own, gen_char_star = load_generated()

    # 读头像
    conn = pymysql.connect(**DB)
    c = conn.cursor()
    avatar_map = {}
    c.execute("SELECT char_name, avatar FROM dim_role")
    for row in c.fetchall(): avatar_map[row[0]] = row[1] or ""

    version = "6.6深渊使用率统计(第一期)"
    c.execute("DELETE FROM ads_team_usage WHERE version_name=%s", (version,))
    c.execute("DELETE FROM ads_char_summary WHERE version_name=%s", (version,))

    # ── 融合配队 ──
    team_sql = """INSERT INTO ads_team_usage
        (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
        VALUES (%s,%s,%s,%s,%s,%s)"""
    team_rows = 0
    all_team_keys = set()

    # 先写基底所有配队
    for name, roles, _, use_r, has_r in base_teams:
        avatars = [avatar_map.get(r, "") for r in roles]
        gen_key = None
        for g_name, g_roles, _, g_use, g_has in gen_teams:
            if set(g_roles) == set(roles):
                gen_key = (g_use, g_has)
                break
        final_use = weighted_merge(use_r, gen_key[0]) if gen_key else use_r
        final_has = weighted_merge(has_r, gen_key[1]) if gen_key else has_r
        c.execute(team_sql, (version, name, json.dumps(roles, ensure_ascii=False),
                   json.dumps(avatars, ensure_ascii=False), final_use, final_has))
        team_rows += 1
        all_team_keys.add(name)

    # 再补生成器独有的新配队
    for name, roles, _, use_r, has_r in gen_teams:
        if name in all_team_keys: continue
        avatars = [avatar_map.get(r, "") for r in roles]
        c.execute(team_sql, (version, name, json.dumps(roles, ensure_ascii=False),
                   json.dumps(avatars, ensure_ascii=False), use_r, has_r))
        team_rows += 1

    # ── 融合角色 ──
    char_sql = """INSERT INTO ads_char_summary
        (version_name, char_name, star, use_rate, own_rate, avg_constellation, avg_level)
        VALUES (%s,%s,%s,%s,%s,1.0,90)"""
    char_rows = 0
    all_chars = set(base_char_use.keys()) | set(gen_char_use.keys())
    for name in all_chars:
        final_use = weighted_merge(base_char_use.get(name, 0), gen_char_use.get(name, 0))
        final_own = weighted_merge(base_char_own.get(name, 0), gen_char_own.get(name, 0))
        star = base_char_star.get(name) or gen_char_star.get(name, 4)
        c.execute(char_sql, (version, name, star, final_use, final_own))
        char_rows += 1

    conn.commit(); c.close(); conn.close()
    print(f"[Abyss-Merge] 基底 {len(base_teams)} 队 × {BASE_WEIGHT} + 生成 {len(gen_teams)} 队 × {GEN_WEIGHT} → {team_rows} 队 + {char_rows} 角色", file=sys.stderr)

if __name__ == "__main__":
    main()
