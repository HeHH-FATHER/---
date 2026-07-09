#!/usr/bin/env python3
"""
从原始 API 数据构建 StatsDocument JSON → 供 Abyss-Record-Generator 使用
输入: 深渊角色使用率.json + 深渊配队汇总.json + 深渊梯度排名.json
输出: abyss_stats.json (StatsDocument 格式)
"""
import json, sys

def build(char_file, team_file, tier_file, out_file):
    # 1. 加载角色
    with open(char_file, 'r', encoding='utf-8') as f:
        chars_raw = json.load(f)
    chars = []
    for c in chars_raw:
        chars.append({
            "name": c.get("name", "?"),
            "star": c.get("star", 4),
            "use_count": c.get("use_count", 0),
            "own_count": c.get("own_count", 0),
            "use_rate": c.get("use_rate", 0),
            "own_rate": c.get("own_rate", 0),
            "constellation": round(c.get("avg_constellation", 0), 1)
        })

    # 2. 加载配队（支持数组或 {"teams": [...]} 格式）
    teams = []
    try:
        with open(team_file, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        teams_raw = raw if isinstance(raw, list) else raw.get("teams", [])
        for t in teams_raw[:100]:
            members = []
            roles = t.get("roles") or t.get("members") or []
            if isinstance(roles, str):
                try: roles = json.loads(roles)
                except: roles = []
            for r in roles:
                name = r if isinstance(r, str) else r.get("name", r.get("role", "?"))
                members.append({"name": name})
            if members:
                # 用 use_rate 作为权重（不存在则用 use_count，默认 1）
                weight = t.get("use_count", t.get("count", 0))
                if weight == 0:
                    weight = int(t.get("use_rate", 0) * 100) or 1
                teams.append({
                    "name": t.get("name", f"队伍{len(teams)+1}"),
                    "use_count": weight,
                    "members": members
                })
    except Exception as e:
        print(f"  [WARN] 配队文件解析失败，使用空列表: {e}")

    # 3. 加载梯队
    tiers = []
    try:
        with open(tier_file, 'r', encoding='utf-8') as f:
            tiers_raw = json.load(f)
        # 可能是 {"S+": [...], "S": [...], ...} dict 或 [{"tier":"S+", "chars":[...]}] list
        if isinstance(tiers_raw, dict):
            for tier_name, tier_chars in tiers_raw.items():
                char_list = []
                for tc in tier_chars:
                    name = tc if isinstance(tc, str) else tc.get("name", "?")
                    char_list.append({"name": name, "constellation": tc.get("constellation", 0) if isinstance(tc, dict) else 0})
                tiers.append({"name": tier_name, "chars": char_list})
        elif isinstance(tiers_raw, list):
            for t in tiers_raw:
                tier_name = t.get("tier", t.get("name", "?"))
                char_list = []
                for tc in t.get("chars", []):
                    char_list.append({"name": tc.get("name", tc if isinstance(tc, str) else "?"), "constellation": tc.get("constellation", 0) if isinstance(tc, dict) else 0})
                tiers.append({"name": tier_name, "chars": char_list})
    except Exception as e:
        print(f"  [WARN] 梯队文件解析失败: {e}")

    # 4. 组装 StatsDocument
    doc = {
        "samples": 61266,  # 从深渊元数据中的 valid_samples
        "char_count": len(chars),
        "team_count": len(teams),
        "tier_count": len(tiers),
        "chars": chars,
        "teams": teams,
        "tiers": tiers
    }

    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)

    print(f"[OK] StatsDocument → {out_file}")
    print(f"  chars: {len(chars)}, teams: {len(teams)}, tiers: {len(tiers)}")

if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "提瓦特数据"
    build(f"{base}/深渊角色使用率.json", f"{base}/深渊配队汇总.json",
          f"{base}/深渊梯度排名.json", "abyss_stats.json")
