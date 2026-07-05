#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — ADS 层 数据加载（HDFS DWS → MySQL ADS）
═══════════════════════════════════════════════════════════════
层级: ADS（MySQL）— 大屏消费表，整个链路最后一层
上游: DWS 层 CSVs on HDFS

加载目标:
  ads_meta_ranking   — 使用率红黑榜（TOP10红榜 + BOTTOM10黑榜）
  ads_char_trend     — 角色长青榜（TOP15角色全版本趋势）
  ads_team_network   — 配队共现网络（TOP30边）

用法:
  python load_ads_to_mysql.py [--version v6.6(第一期)]
                              [--host 100.103.177.85]
                              [--user root] [--password 123456]
                              [--database abyss_db]
═══════════════════════════════════════════════════════════════
"""

import sys
import os
import subprocess
import argparse
import json
import tempfile
try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

# ═══════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════

DEFAULT_DB = {
    "host": "100.103.177.85",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "abyss_db",
    "charset": "utf8mb4",
}

HDFS_DWS_CHAR   = "/data/abyss/dws/dws_char_summary/"
HDFS_DWS_COOCCUR = "/data/abyss/dws/dws_team_cooccur/"


# ═══════════════════════════════════════════════
# HDFS 读取
# ═══════════════════════════════════════════════

def hdfs_read_csv(hdfs_dir):
    """读取 HDFS 目录下所有 part-* 文件内容，返回行列表"""
    try:
        # 尝试多种通配符模式
        for pattern in [
            hdfs_dir.rstrip("/") + "/part-*",
            hdfs_dir.rstrip("/") + "/*",
        ]:
            result = subprocess.run(
                ["hdfs", "dfs", "-cat"] + [pattern],
                capture_output=True, text=True, timeout=120,
                shell=True  # 通配符需要 shell 展开
            )
            if result.returncode == 0 and result.stdout.strip():
                return [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]

        return []
    except Exception as e:
        print(f"[ERROR] HDFS 读取失败: {e}")
        return []


# ═══════════════════════════════════════════════
# MySQL 连接
# ═══════════════════════════════════════════════

def get_connection(db_config):
    """获取 MySQL 连接"""
    if HAS_PYMYSQL:
        return pymysql.connect(**db_config)
    else:
        print("[FATAL] 需要 pymysql: pip install pymysql")
        sys.exit(1)


# ═══════════════════════════════════════════════
# 1. ads_meta_ranking — 红黑榜
# ═══════════════════════════════════════════════

def load_meta_ranking(conn, version, char_summary_rows):
    """
    DWS char_summary → ads_meta_ranking
    取最新版本的 TOP10(红榜) + BOTTOM10(黑榜)，按 use_rate 排名
    """
    print("\n[ADS-1] 构建 ads_meta_ranking...")

    # 解析 DWS CSV: version,char_name,star,own_count,use_count,total_users,own_rate,use_rate,avg_constellation,avg_level
    records = []
    for line in char_summary_rows:
        parts = line.split(",", 9)
        if len(parts) < 9 or parts[0].startswith("version"):
            continue
        # CSV 中 key=version,char_name → parts[0]="v6.6(第一期),玛薇卡"
        # 需要重新解析——实际上 reducer 输出的 key 是 "version,char_name"
        # value 是 "star,own_count,..."
        # part-r-00000 格式: key\value (Tab 分隔)
        pass

    # AbyssAggMR 输出格式: Key\tValue (TextOutputFormat)
    # 解析
    parsed = []
    for line in char_summary_rows:
        if "\t" in line:
            key, val = line.split("\t", 1)
            # key = "version,char_name"
            key_parts = key.split(",", 1)
            if len(key_parts) < 2:
                continue
            ver = key_parts[0].strip()
            name = key_parts[1].strip()
            # 如果指定了版本过滤
            if version and ver != version:
                continue
        else:
            # 可能是逗号格式
            parts = line.split(",", 9)
            if len(parts) < 9:
                continue
            ver = parts[0].strip()
            name = parts[1].strip()
            if version and ver != version:
                continue
            val = ",".join(parts[2:])

        val_parts = val.split(",", 8)
        if len(val_parts) < 8:
            continue
        try:
            star = int(val_parts[0])
            own_count = int(val_parts[1])
            use_count = int(val_parts[2])
            total_users = int(val_parts[3])
            own_rate = float(val_parts[4])
            use_rate = float(val_parts[5])
            avg_const = float(val_parts[6])
            avg_level = float(val_parts[7])
        except (ValueError, IndexError):
            continue

        parsed.append({
            "version": ver,
            "char_name": name,
            "star": star,
            "use_rate": use_rate,
            "own_rate": own_rate,
        })

    if not parsed:
        print("  ⚠ 无数据（请确认 DWS 聚合已完成）")
        return 0

    # 按 use_rate 降序排列
    parsed.sort(key=lambda x: -x["use_rate"])
    total = len(parsed)

    cursor = conn.cursor()

    # 清空旧数据
    cursor.execute("TRUNCATE TABLE ads_meta_ranking")

    sql = """INSERT INTO ads_meta_ranking (rank_num, char_name, star, avatar, use_rate, own_rate, list_type)
             VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    count = 0
    # 红榜 TOP10
    for i, r in enumerate(parsed[:10]):
        cursor.execute(sql, (i + 1, r["char_name"], r["star"], None,
                             r["use_rate"], r["own_rate"], "red"))
        count += 1

    # 黑榜 BOTTOM10（倒数第一编号为 total）
    bottom = parsed[-10:] if total >= 10 else []
    for i, r in enumerate(bottom):
        rank = total - len(bottom) + i + 1
        cursor.execute(sql, (rank, r["char_name"], r["star"], None,
                             r["use_rate"], r["own_rate"], "black"))
        count += 1

    conn.commit()
    cursor.close()
    print(f"  ✓ ads_meta_ranking: {count} 条 (红榜TOP10 + 黑榜BOTTOM{len(bottom)})")
    return count


# ═══════════════════════════════════════════════
# 2. ads_char_trend — 角色长青榜
# ═══════════════════════════════════════════════

def load_char_trend(conn, char_summary_rows):
    """
    DWS char_summary → ads_char_trend
    TOP15角色：各版本 use_rate 趋势（JSON 数组格式存 MySQL）
    """
    print("\n[ADS-2] 构建 ads_char_trend...")

    # 解析所有数据，按角色聚合版本趋势
    char_trends = {}  # {char_name: {"star": int, "versions": [], "rates": []}}

    for line in char_summary_rows:
        if "\t" not in line:
            continue
        key, val = line.split("\t", 1)
        key_parts = key.split(",", 1)
        if len(key_parts) < 2:
            continue
        ver = key_parts[0].strip()
        name = key_parts[1].strip()

        val_parts = val.split(",", 8)
        if len(val_parts) < 8:
            continue
        try:
            star = int(val_parts[0])
            use_rate = float(val_parts[5])
        except (ValueError, IndexError):
            continue

        if name not in char_trends:
            char_trends[name] = {"star": star, "pairs": []}
        char_trends[name]["pairs"].append((ver, use_rate))

    if not char_trends:
        print("  ⚠ 无数据")
        return 0

    # 选 TOP15（按最新版本 use_rate）
    sorted_chars = []
    for name, data in char_trends.items():
        data["pairs"].sort(key=lambda x: x[0])  # 按版本排序
        latest_rate = data["pairs"][-1][1] if data["pairs"] else 0
        sorted_chars.append((name, data, latest_rate))

    sorted_chars.sort(key=lambda x: -x[2])
    top15 = sorted_chars[:15]

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE ads_char_trend")

    sql = """INSERT INTO ads_char_trend (char_name, star, avatar, version_list, rate_list)
             VALUES (%s, %s, %s, %s, %s)"""

    count = 0
    for name, data, _ in top15:
        versions = [p[0] for p in data["pairs"]]
        rates = [p[1] for p in data["pairs"]]

        version_json = json.dumps(versions, ensure_ascii=False)
        rate_json = json.dumps(rates, ensure_ascii=False)

        cursor.execute(sql, (name, data["star"], None, version_json, rate_json))
        count += 1

    conn.commit()
    cursor.close()
    print(f"  ✓ ads_char_trend: {count} 条 (TOP15 角色)")
    return count


# ═══════════════════════════════════════════════
# 3. ads_team_network — 配队共现网络
# ═══════════════════════════════════════════════

def load_team_network(conn):
    """
    DWS team_cooccur → ads_team_network
    TOP30 权重边
    """
    print("\n[ADS-3] 构建 ads_team_network...")

    rows = hdfs_read_csv(HDFS_DWS_COOCCUR)
    if not rows:
        print("  ⚠ 无数据（请先运行 compute_team_cooccur.py）")
        return 0

    # CSV: char_a,char_b,cooccur_count
    edges = []
    for line in rows:
        if line.startswith("char_a"):
            continue
        parts = line.split(",", 2)
        if len(parts) < 3:
            continue
        edges.append({
            "source": parts[0].strip(),
            "target": parts[1].strip(),
            "weight": int(parts[2].strip()),
        })

    # TOP30
    edges.sort(key=lambda x: -x["weight"])
    top30 = edges[:30]

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE ads_team_network")

    sql = """INSERT INTO ads_team_network (source_name, target_name, source_avatar, target_avatar, weight)
             VALUES (%s, %s, %s, %s, %s)"""

    count = 0
    for e in top30:
        cursor.execute(sql, (e["source"], e["target"], None, None, e["weight"]))
        count += 1

    conn.commit()
    cursor.close()
    print(f"  ✓ ads_team_network: {count} 条 (TOP30 边)")
    return count


# ═══════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="逐风平台 — ADS 层 数据加载 (HDFS DWS → MySQL ADS)"
    )
    parser.add_argument("--version", "-v", default=None,
                        help="版本过滤，如 v6.6(第一期)（不指定则加载全版本）")
    parser.add_argument("--host", default=DEFAULT_DB["host"])
    parser.add_argument("--port", type=int, default=DEFAULT_DB["port"])
    parser.add_argument("--user", default=DEFAULT_DB["user"])
    parser.add_argument("--password", default=DEFAULT_DB["password"])
    parser.add_argument("--database", default=DEFAULT_DB["database"])
    parser.add_argument("--skip-trend", action="store_true",
                        help="跳过 ads_char_trend")
    parser.add_argument("--skip-network", action="store_true",
                        help="跳过 ads_team_network")

    args = parser.parse_args()

    db_config = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
        "charset": "utf8mb4",
    }

    print("=" * 60)
    print("逐风数据洞察平台 — ADS 层 数据加载")
    print("=" * 60)
    print(f"MySQL: {args.host}:{args.port}/{args.database}")
    print(f"版本:  {args.version or '全版本'}")

    # 连接 MySQL
    conn = get_connection(db_config)

    # 读取 DWS char_summary
    print(f"\n[0] 读取 DWS char_summary: {HDFS_DWS_CHAR}")
    char_rows = hdfs_read_csv(HDFS_DWS_CHAR)
    if not char_rows:
        print("[ERROR] DWS char_summary 为空！请先运行 AbyssAggMR")
        conn.close()
        sys.exit(1)
    print(f"  读取 {len(char_rows)} 行")

    # 依次加载 ADS 表
    total = 0
    total += load_meta_ranking(conn, args.version, char_rows)

    if not args.skip_trend:
        total += load_char_trend(conn, char_rows)

    if not args.skip_network:
        total += load_team_network(conn)

    conn.close()

    print(f"\n══════════════════════════════════════════")
    print(f"ADS 加载完成! 共写入 {total} 条记录")
    print(f"  大屏可访问: http://100.74.215.12:8080/")
    print(f"══════════════════════════════════════════")


if __name__ == "__main__":
    main()
