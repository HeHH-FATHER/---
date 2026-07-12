#!/usr/bin/env python3
"""
批量导入所有版本的角色+配队数据 → MySQL ads_char_summary + ads_team_usage + ads_char_momentum
"""
import json, sys, os
from collections import defaultdict
try: import pymysql
except ImportError: print("[ERROR] pip install pymysql", file=sys.stderr); sys.exit(1)

DB = {"host": "Middleware", "port": 3306, "user": "root", "password": "123456",
      "database": "abyss_db", "charset": "utf8mb4"}

# 版本映射：文件名前缀 → version_name
VER_MAP = {
    "v55": "6.3深渊使用率统计(第二期)",
    "v56": "6.4深渊使用率统计(第一期)",
    "v57": "6.5深渊使用率统计(第一期)",
    "v58": "6.5深渊使用率统计(第二期)",
    "v59": "6.6深渊使用率统计(第一期)",
}
# 版本顺序（用于涨跌对比）
VER_ORDER = ["v55", "v56", "v57", "v58", "v59"]

def load_file(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)

def main(base_dir):
    if not os.path.isdir(base_dir):
        print(f"Usage: python3 import_all_versions.py <dir with version JSONs>", file=sys.stderr)
        return

    conn = pymysql.connect(**DB)
    c = conn.cursor()

    # 清空旧数据
    c.execute("DELETE FROM ads_char_summary")
    c.execute("DELETE FROM ads_team_usage")
    c.execute("DELETE FROM ads_char_momentum")

    avatar_map = {}
    c.execute("SELECT char_name, avatar FROM dim_role")
    for row in c.fetchall(): avatar_map[row[0]] = row[1] or ""

    # 角色使用率（用于计算涨跌）
    ver_char_use = {}  # version_name → {char_name → use_rate}

    for prefix in VER_ORDER:
        ver_name = VER_MAP[prefix]
        file_path = os.path.join(base_dir, f"{prefix}_{ver_name}.json")
        if not os.path.exists(file_path):
            # try matching any file starting with prefix
            for fn in os.listdir(base_dir):
                if fn.startswith(prefix):
                    file_path = os.path.join(base_dir, fn)
                    break
        if not os.path.exists(file_path):
            print(f"[SKIP] {prefix}: file not found", file=sys.stderr)
            continue

        data = load_file(file_path)
        chars = data.get("chars", [])
        teams = data.get("teams", data if isinstance(data, list) else [])

        # ── 角色 ──
        char_use = {}
        char_sql = """INSERT INTO ads_char_summary
            (version_name, char_name, star, use_rate, own_rate, avg_constellation, avg_level, use_count, own_count)
            VALUES (%s,%s,%s,%s,%s,1.0,90,1,1)
            ON DUPLICATE KEY UPDATE use_rate=VALUES(use_rate), own_rate=VALUES(own_rate), star=VALUES(star)"""

        if chars:
            for ch in chars:
                name = ch.get("name", "?")
                use_r = ch.get("use_rate", 0)
                own_r = ch.get("own_rate", 0)
                star = ch.get("star", 4)
                c.execute(char_sql, (ver_name, name, star, use_r, own_r))
                char_use[name] = use_r
        ver_char_use[ver_name] = char_use

        # ── 配队 ──
        team_sql = """INSERT INTO ads_team_usage
            (version_name, team_name, roles_json, avatars_json, use_rate, has_rate)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE use_rate=VALUES(use_rate), has_rate=VALUES(has_rate)"""

        if teams:
            for t in teams:
                members = t.get("members", [])
                if not members: continue
                names = [m.get("name", "?") for m in members]
                avatars = [m.get("avatar", "") for m in members]
                team_name = "+".join(names)
                use_r = t.get("use_rate", 0)
                has_r = t.get("has_rate", 0)
                c.execute(team_sql, (ver_name, team_name, json.dumps(names, ensure_ascii=False),
                           json.dumps(avatars, ensure_ascii=False), use_r, has_r))

        print(f"[OK] {ver_name}: {len(char_use)} chars + {len(teams)} teams", file=sys.stderr)

    # ── 涨跌（相邻版本对比）──
    c.execute("DELETE FROM ads_char_momentum")
    for i in range(1, len(VER_ORDER)):
        prev = VER_MAP[VER_ORDER[i-1]]
        curr = VER_MAP[VER_ORDER[i]]
        prev_chars = ver_char_use.get(prev, {})
        curr_chars = ver_char_use.get(curr, {})

        all_names = set(prev_chars.keys()) & set(curr_chars.keys())
        for name in all_names:
            prev_r = prev_chars[name]
            curr_r = curr_chars[name]
            trend = round(curr_r - prev_r, 1)
            if trend == 0: continue
            c.execute("INSERT INTO ads_char_momentum (version_name, char_name, prev_rate, curr_rate, trend, avatar) VALUES (%s,%s,%s,%s,%s,%s)",
                      (curr, name, prev_r, curr_r, trend, avatar_map.get(name, "")))
        print(f"[Momentum] {prev}→{curr}: {len(all_names)} chars compared", file=sys.stderr)

    conn.commit(); c.close(); conn.close()
    print("[DONE] 全部导入完成", file=sys.stderr)

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "提瓦特数据/原始数据")
