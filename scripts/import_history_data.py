#!/usr/bin/env python3
"""
导入历史深渊数据到 MySQL
来源: _abyss_data.js + 历代卡池抽取比例.html
目标: ads_char_summary + ads_team_usage + dim_banner
"""
import json, re, sys

HOST = "100.103.177.85"
USER = "root"
PASS = "123456"
DB = "abyss_db"

def get_conn():
    import pymysql
    return pymysql.connect(host=HOST, user=USER, password=PASS, database=DB, charset="utf8mb4")

# ========== 1. 导入 _abyss_data.js → ads_char_summary ==========
def import_abyss():
    print("[1] 导入深渊角色数据...")
    with open("C:/Users/82165/Desktop/md/数据分析/_abyss_data.js", "r", encoding="utf-8") as f:
        raw = f.read()

    # Parse JSON from JS
    start = raw.find("[")
    depth = 0
    for i in range(start, len(raw)):
        if raw[i] == '[': depth += 1
        elif raw[i] == ']':
            depth -= 1
            if depth == 0:
                data = json.loads(raw[start:i+1])
                break

    conn = get_conn()
    c = conn.cursor()

    # 先清空旧数据（保留 v6.6第一期的管道产出）
    c.execute("DELETE FROM ads_char_summary WHERE version_name != 'v6.6(第一期)'")

    sql = """INSERT INTO ads_char_summary
             (version_name, char_name, star, own_count, use_count, total_users, own_rate, use_rate, avg_constellation)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    total = 0
    for ver in data:
        vname = ver["t"]
        samples = ver["s"]
        for ch in ver["chars"]:
            uc = ch.get("uc", 0)
            oc = ch.get("oc", samples)
            use_rate = round(uc / samples * 100, 2) if samples > 0 else 0
            own_rate = round(ch.get("or", 0), 2)
            c.execute(sql, (vname, ch["n"], ch["st"], oc, uc, samples, own_rate, use_rate, round(ch.get("co", 0), 1)))
            total += 1

    conn.commit()
    print(f"  ads_char_summary: {total} 行 (共 {len(data)} 个版本)")

    # ====== 导入 teams ======
    print("[2] 导入配队数据...")
    c.execute("DELETE FROM ads_team_usage WHERE id NOT IN (SELECT id FROM (SELECT id FROM ads_team_usage LIMIT 10) t)")

    sql2 = """INSERT INTO ads_team_usage (team_name, roles_json, avatars_json, use_rate, has_rate)
              VALUES (%s,%s,%s,%s,%s)"""

    team_total = 0
    for ver in data:
        for tm in ver.get("teams", [])[:50]:  # TOP50 per version
            names = [m["n"] for m in tm["m"]]
            avatars = [m["a"] for m in tm["m"]]
            team_name = " + ".join(names)
            c.execute(sql2, (team_name, json.dumps(names, ensure_ascii=False),
                           json.dumps(avatars, ensure_ascii=False),
                           round(tm.get("ur", 0), 1), round(tm.get("hr", 0), 1)))
            team_total += 1

    conn.commit()
    print(f"  ads_team_usage: {team_total} 行")
    conn.close()

# ========== 2. 导入 历代卡池抽取比例.html → dim_banner ==========
def import_banners():
    print("[3] 导入卡池数据...")
    with open("C:/Users/82165/Desktop/md/数据分析/历代卡池抽取比例.html", "r", encoding="utf-8") as f:
        content = f.read()

    def extract_js_array_text(var_name):
        """Extract JavaScript array as text"""
        start = content.find(f"const {var_name} = [")
        if start == -1: return ""
        start = content.find("[", start)
        depth = 0
        for i in range(start, len(content)):
            if content[i] == '[': depth += 1
            elif content[i] == ']':
                depth -= 1
                if depth == 0:
                    return content[start:i+1]
        return ""

    # Parse using eval (safe, our own data)
    import ast
    def safe_parse(js_text):
        """Parse JS object with unquoted keys to Python dict"""
        # Replace unquoted keys with quoted ones
        js_text = re.sub(r'(\w+):', r'"\1":', js_text)
        return json.loads(js_text)

    all_records = []
    for var_name, banner_type in [("ROLE_LIST", "角色池"), ("WEAPON_LIST", "武器池"), ("HYBRID_LIST", "混池")]:
        text = extract_js_array_text(var_name)
        if not text:
            print(f"  {var_name}: 未找到")
            continue
        items = safe_parse(text)
        for item in items:
            ver = item["version"]
            start = item["start"]
            end = item["end"]
            for char_name, pull_count in item["content"].items():
                all_records.append((ver, banner_type, start, end, char_name, pull_count))
        print(f"  {var_name}: {len(items)} 期")

    conn = get_conn()
    c = conn.cursor()
    # 删旧的武器池/混池（角色池保留管道的）
    c.execute("DELETE FROM dim_banner WHERE banner_type IN ('武器池','混池')")

    sql = "INSERT IGNORE INTO dim_banner (version_name, banner_type, start_time, end_time, char_name, pull_count) VALUES (%s,%s,%s,%s,%s,%s)"
    for r in all_records:
        c.execute(sql, r)

    conn.commit()
    print(f"  dim_banner: {len(all_records)} 行")
    conn.close()

if __name__ == "__main__":
    import_abyss()
    import_banners()
    print("\n导入完成!")
