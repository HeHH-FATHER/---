#!/usr/bin/env python3
"""
逐风数据洞察平台 — 角色版本涨跌异动计算
==========================================
从深渊配队汇总.json 提取最新两期，按角色聚合使用率，
归一化后计算跨版本涨跌幅 → 写入 ads_char_momentum 表。

用法:
  python3 scripts/compute_momentum.py [--host 100.103.177.85] [--user root] [--pass 123456] [--db abyss_db]
"""
import json
import sys
import argparse
from collections import defaultdict

try:
    import pymysql
except ImportError:
    print("[momentum] 需要 pymysql: pip3 install pymysql")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="角色版本涨跌异动计算")
    parser.add_argument("--host", default="100.103.177.85")
    parser.add_argument("--user", default="root")
    parser.add_argument("--pass", dest="password", default="123456")
    parser.add_argument("--db", default="abyss_db")
    parser.add_argument("--source", default="提瓦特数据/深渊配队汇总.json",
                        help="配队汇总数据源")
    parser.add_argument("--min-trend", type=float, default=1.0,
                        help="最小涨跌幅阈值(%), 低于此值不入库")
    args = parser.parse_args()

    # ── 1. 读取配队数据 ──
    try:
        with open(args.source, "r", encoding="utf-8") as f:
            teams = json.load(f)
    except FileNotFoundError:
        print(f"[momentum] 数据源不存在: {args.source}")
        sys.exit(1)

    # ── 2. 按版本聚合角色使用率 ──
    ver_char = defaultdict(lambda: defaultdict(float))
    for t in teams:
        v = t["version"]
        rate = float(t.get("use_rate", 0))
        for role in t.get("roles", []):
            ver_char[v][role] += rate

    versions = sorted(ver_char.keys(), reverse=True)
    if len(versions) < 2:
        print("[momentum] 版本数不足2, 跳过")
        sys.exit(0)

    latest, prev = versions[0], versions[1]
    print(f"[momentum] 最新: {latest} ({len(ver_char[latest])}角色)")
    print(f"[momentum] 上期: {prev} ({len(ver_char[prev])}角色)")

    # ── 3. 归一化（每个版本 max=100%） ──
    def normalize(d):
        m = max(d.values()) if d else 1
        return {k: round(v / m * 100, 1) for k, v in d.items()}

    norm_latest = normalize(ver_char[latest])
    norm_prev = normalize(ver_char[prev])

    # ── 4. 计算涨跌幅 ──
    trends = []
    all_chars = set(list(norm_latest.keys()) + list(norm_prev.keys()))
    for name in all_chars:
        cur = norm_latest.get(name, 0)
        prv = norm_prev.get(name, 0)
        trend = round(cur - prv, 1)
        if abs(trend) >= args.min_trend:
            trends.append((name, prv, cur, trend))

    trends.sort(key=lambda x: -x[3])
    print(f"[momentum] 有效涨跌角色: {len(trends)} (|涨跌|≥{args.min_trend}%)")

    # ── 5. 写入 MySQL ──
    conn = pymysql.connect(
        host=args.host, port=3306, user=args.user,
        password=args.password, database=args.db, charset="utf8mb4"
    )
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS ads_char_momentum (
        id INT(11) AUTO_INCREMENT PRIMARY KEY,
        char_name VARCHAR(50), prev_rate DECIMAL(5,1), curr_rate DECIMAL(5,1),
        trend DECIMAL(5,1), avatar VARCHAR(500),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    c.execute("DELETE FROM ads_char_momentum")

    inserted = 0
    for name, prv, cur, trend in trends:
        c.execute("SELECT avatar FROM dim_role WHERE char_name=%s LIMIT 1", (name,))
        row = c.fetchone()
        av = row[0] if row else ""
        c.execute(
            "INSERT INTO ads_char_momentum (char_name,prev_rate,curr_rate,trend,avatar) VALUES (%s,%s,%s,%s,%s)",
            (name, prv, cur, trend, av),
        )
        inserted += 1

    conn.commit()
    c.close()
    conn.close()

    # 摘要
    print(f"[momentum] 入库: {inserted} 条")
    print(f"[momentum] ▲TOP3: " + ", ".join(
        f"{t[0]}+{t[3]}" for t in trends[:3]
    ))
    print(f"[momentum] ▼BOT3: " + ", ".join(
        f"{t[0]}{t[3]}" for t in trends[-3:]
    ))


if __name__ == "__main__":
    main()
