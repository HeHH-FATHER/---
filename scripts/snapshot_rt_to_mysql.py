#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — 实时链数据留存（Redis → MySQL）
═══════════════════════════════════════════════════════════════
功能: 定期从 Redis 读取实时链窗口快照，写入 MySQL rt_* 表。
       解决实时数据"闪一下就没了"的问题，支持后续与离线聚合做交叉验证。

写入目标:
  rt_gacha_result   — 抽卡聚合快照（每 N 分钟）
  rt_build_snapshot  — 练度快照（每 N 分钟）

Redis Key 对应关系:
  gacha:*      → rt_gacha_result
  build:*      → rt_build_snapshot

用法:
  python snapshot_rt_to_mysql.py                  # 执行一次快照
  python snapshot_rt_to_mysql.py --loop 300       # 每300秒循环执行
  python snapshot_rt_to_mysql.py --once --verbose # 单次详细输出
═══════════════════════════════════════════════════════════════
"""

import sys
import os
import time
import argparse
import json
from datetime import datetime

try:
    import redis
    import pymysql
except ImportError:
    print("[FATAL] 需要 redis 和 pymysql: pip install redis pymysql")
    sys.exit(1)

# ═══════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════

REDIS_HOST = os.environ.get("REDIS_HOST", "100.103.177.85")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB   = int(os.environ.get("REDIS_DB", 0))

MYSQL_HOST = os.environ.get("MYSQL_HOST", "100.103.177.85")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "123456")
MYSQL_DB   = os.environ.get("MYSQL_DB", "abyss_db")

# ═══════════════════════════════════════════
# Redis 连接
# ═══════════════════════════════════════════

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                       decode_responses=True, socket_connect_timeout=5)

# ═══════════════════════════════════════════
# 抽卡快照：gacha:* → rt_gacha_result
# ═══════════════════════════════════════════

def snapshot_gacha(r: redis.Redis, conn: pymysql.Connection, verbose=False):
    """
    Redis → rt_gacha_result

    Redis key:
      gacha:pull_count  (String) — 窗口抽取总次数
      gacha:five_star   (String) — 五星出货率(%)
      gacha:top_char    (String) — 最热角色名
    """
    # 读取 Redis
    pull_count = r.get("gacha:pull_count")
    five_star  = r.get("gacha:five_star")
    top_char   = r.get("gacha:top_char")

    if verbose:
        print(f"  [gacha] pull_count={pull_count}, five_star={five_star}, top_char={top_char}")

    if pull_count is None:
        return 0

    try:
        pull_count = int(pull_count)
    except (ValueError, TypeError):
        return 0

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 写入抽卡总次数
    sql = """INSERT INTO rt_gacha_result (char_name, pull_count, window_time)
             VALUES (%s, %s, %s)"""
    cursor.execute(sql, ("__ALL__", pull_count, now))

    # 写入 TOP 角色（如果 Redis 存的是 JSON 列表则解析）
    if top_char:
        try:
            chars = json.loads(top_char) if top_char.startswith("[") else [top_char]
        except (json.JSONDecodeError, ValueError):
            chars = [top_char]
        for i, c in enumerate(chars[:10]):
            cursor.execute(sql, (str(c), 0, now))  # pull_count 暂填0，后续可从 Redis 细化

    conn.commit()
    cursor.close()
    count = 1 + (len(chars) if top_char else 0)
    if verbose:
        print(f"  [gacha] 写入 {count} 条")
    return count

# ═══════════════════════════════════════════
# 练度快照：build:* → rt_build_snapshot
# ═══════════════════════════════════════════

def snapshot_hot_chars(r: redis.Redis, conn: pymysql.Connection, verbose=False):
    """build:hot_chars JSON → rt_build_snapshot（含命座分布）"""
    json_str = r.get("build:hot_chars")
    if not json_str:
        return 0

    try:
        hot_list = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return 0

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0

    for item in hot_list[:4]:  # TOP4
        role = item.get("role", "?")
        avg_const = item.get("avg_constellation", 0)
        avg_dmg = item.get("avg_damage", 0)
        const_dist = json.dumps(item.get("constellation_dist", {}))
        weapons = json.dumps([w.get("name") for w in (item.get("weapons") or [])[:3]])
        artifacts = json.dumps([a.get("name") for a in (item.get("artifacts") or [])[:3]])

        sql = """INSERT INTO rt_build_snapshot
                 (char_name, window_time, avg_constellation, avg_damage, top_weapon, top_artifact, const_dist, weapons_json, artifacts_json)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON DUPLICATE KEY UPDATE
                   avg_constellation=VALUES(avg_constellation), avg_damage=VALUES(avg_damage),
                   const_dist=VALUES(const_dist), top_weapon=VALUES(top_weapon), top_artifact=VALUES(top_artifact)"""
        try:
            cursor.execute(sql, (role, now, float(avg_const), int(avg_dmg),
                                 weapons, artifacts, const_dist, weapons, artifacts))
            count += 1
        except Exception as e:
            if verbose:
                print(f"  [hot_chars] {role} 写入失败: {e}")

    conn.commit()
    cursor.close()
    if verbose:
        print(f"  [hot_chars] TOP{len(hot_list)} 写入 {count} 条")
    return count


def snapshot_build(r: redis.Redis, conn: pymysql.Connection, verbose=False):
    """
    Redis → rt_build_snapshot

    Redis key:
      build:avg_const  (String) — 平均命座
      build:top_weapon (String) — TOP武器名
      build:top_arti   (String) — TOP圣遗物名
      build:avg_damage (String) — 平均伤害
    """
    avg_const = r.get("build:avg_const")
    top_weapon = r.get("build:top_weapon")
    top_arti  = r.get("build:top_arti")
    avg_damage = r.get("build:avg_damage")

    if verbose:
        print(f"  [build] avg_const={avg_const}, top_weapon={top_weapon}, top_arti={top_arti}")

    if not any([avg_const, top_weapon, top_arti, avg_damage]):
        return 0

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sql = """INSERT INTO rt_build_snapshot
             (char_name, window_time, avg_constellation, avg_damage, top_weapon, top_artifact)
             VALUES (%s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql, (
        "__ALL__",
        now,
        float(avg_const) if avg_const else None,
        int(avg_damage) if avg_damage else None,
        top_weapon or "",
        top_arti or ""
    ))

    conn.commit()
    cursor.close()
    if verbose:
        print(f"  [build] 写入 1 条")
    return 1

# ═══════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════

def run_snapshot(verbose=False):
    """执行一次完整快照"""
    r = get_redis()
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, charset="utf8mb4"
    )

    total = 0
    try:
        total += snapshot_gacha(r, conn, verbose)
    except Exception as e:
        print(f"  [ERROR] gacha snapshot: {e}")

    try:
        total += snapshot_build(r, conn, verbose)
    except Exception as e:
        print(f"  [ERROR] build snapshot: {e}")

    try:
        total += snapshot_hot_chars(r, conn, verbose)
    except Exception as e:
        print(f"  [ERROR] hot_chars snapshot: {e}")

    conn.close()
    r.close()
    return total

def main():
    parser = argparse.ArgumentParser(description="逐风平台 — 实时链数据留存 (Redis → MySQL)")
    parser.add_argument("--loop", type=int, default=0,
                        help="循环间隔(秒)，0=只执行一次")
    parser.add_argument("--verbose", action="store_true",
                        help="详细输出")
    args = parser.parse_args()

    if args.loop > 0:
        print(f"[RT Snapshot] 启动循环，间隔 {args.loop} 秒，Ctrl+C 停止")
        while True:
            try:
                n = run_snapshot(args.verbose)
                ts = datetime.now().strftime("%H:%M:%S")
                if n > 0 or args.verbose:
                    print(f"  [{ts}] 快照完成: {n} 条")
                time.sleep(args.loop)
            except KeyboardInterrupt:
                print("\n[RT Snapshot] 停止")
                break
            except Exception as e:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] ERROR: {e}")
                time.sleep(args.loop)
    else:
        n = run_snapshot(verbose=True)
        print(f"\n[RT Snapshot] 完成: {n} 条")

if __name__ == "__main__":
    main()
