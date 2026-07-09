#!/usr/bin/env python3
"""
逐风数据洞察平台 — 热门角色练度聚合（旁路消费者）
==================================================
独立消费 Kafka build-v2，使用新 consumer group，不影响已有链路。
60 秒滑动窗口按角色聚合 → 每 3 秒写入 Redis build:hot_chars。

用法:
  python3 scripts/hot_char_aggregator.py
  nohup python3 scripts/hot_char_aggregator.py > /tmp/hot_char.log 2>&1 &
"""
import json
import time
import threading
from collections import defaultdict, deque

try:
    from kafka import KafkaConsumer
    import redis
    import pymysql
except ImportError:
    print("需要: pip install kafka-python redis pymysql")
    exit(1)

# ==================== 配置 ====================
KAFKA_BOOTSTRAP = "Middleware:9092"
REDIS_HOST = "Middleware"
REDIS_PORT = 6379
WINDOW_SECS = 60       # 滑动窗口 60 秒
FLUSH_INTERVAL = 3      # 每 3 秒写一次 Redis
TOP_N = 4               # 取 TOP 4 热门角色

# MySQL 落库配置
MYSQL_HOST = "Middleware"
MYSQL_USER = "root"
MYSQL_PASS = "123456"
MYSQL_DB   = "abyss_db"
MYSQL_INTERVAL = 7200   # 每 2 小时落一次 MySQL
MYSQL_RETENTION = 43200 # 保留 12 小时


def extract_weapon_name(data):
    """从 Kafka 消息中提取武器名（兼容嵌套对象和字符串）"""
    w = data.get("weapon", {})
    if isinstance(w, dict):
        return w.get("name", "?")
    return str(w) if w else "?"


def extract_weapon_icon(data):
    """从 Kafka 消息中提取武器图标 URL"""
    w = data.get("weapon", {})
    if isinstance(w, dict):
        return w.get("avatar", w.get("icon", ""))
    return ""


def extract_arti_name(data):
    """从 Kafka 消息中提取圣遗物名（兼容嵌套对象和字符串）"""
    a = data.get("artifact_set", data.get("artifact", {}))
    if isinstance(a, dict):
        return a.get("name", a.get("set_name", "?"))
    return str(a) if a else "?"


def extract_arti_icon(data):
    """从 Kafka 消息中提取圣遗物图标 URL"""
    a = data.get("artifact_set", data.get("artifact", {}))
    if isinstance(a, dict):
        avatars = a.get("avatars", [])
        return avatars[0] if avatars else ""
    return ""


def main():
    consumer = KafkaConsumer(
        "build-v2",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,   # 不提交偏移，每次重启自动从最新开始
        group_id="python-hot-char-v2"       # ← 新 consumer group，不影响已有链路
    )
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    window = deque()  # [(timestamp, data), ...]
    # 按角色聚合的中间态
    char_stats = defaultdict(lambda: {
        "count": 0,
        "star": 5,
        "constellation_sum": 0,
        "damage_sum": 0,
        "weapons": defaultdict(int),
        "artifacts": defaultdict(int),
        "weapon_icons": {},    # weapon_name → icon_url
        "arti_icons": {},      # arti_name → icon_url
        "avatar": "",          # 角色头像（从消息中取）
        "ename": "",
    })

    last_flush = time.time()
    last_mysql_flush = 0  # 0 表示启动后首次 flush 立即写入
    first_mysql_done = False
    print(f"[HotChar] 旁路消费者启动, 窗口={WINDOW_SECS}s, Redis每{FLUSH_INTERVAL}s, MySQL每{MYSQL_INTERVAL}s")

    for msg in consumer:
        now = time.time()
        data = msg.value
        window.append((now, data))

        # 清理过期数据
        while window and window[0][0] < now - WINDOW_SECS:
            ts, old = window.popleft()
            role = old.get("role", "?")
            st = char_stats[role]
            st["count"] -= 1
            st["constellation_sum"] -= old.get("constellation", 0)
            st["damage_sum"] -= old.get("avg_damage", old.get("damage", 0))
            wname = extract_weapon_name(old)
            st["weapons"][wname] -= 1
            if st["weapons"][wname] <= 0:
                del st["weapons"][wname]
            aname = extract_arti_name(old)
            st["artifacts"][aname] -= 1
            if st["artifacts"][aname] <= 0:
                del st["artifacts"][aname]
            if st["count"] <= 0:
                del char_stats[role]

        # 累加当前消息
        role = data.get("role", "?")
        st = char_stats[role]
        st["count"] += 1
        st["star"] = data.get("star", 5)
        st["constellation_sum"] += data.get("constellation", 0)
        st["damage_sum"] += data.get("avg_damage", data.get("damage", 0))
        wname = extract_weapon_name(data)
        st["weapons"][wname] += 1
        wicon = extract_weapon_icon(data)
        if wicon:
            st["weapon_icons"][wname] = wicon
        aname = extract_arti_name(data)
        st["artifacts"][aname] += 1
        aicon = extract_arti_icon(data)
        if aicon:
            st["arti_icons"][aname] = aicon
        # 角色头像和英文名
        if not st["avatar"]:
            st["avatar"] = data.get("avatar", "")
        if not st["ename"]:
            st["ename"] = data.get("ename", "")

        # 每 FLUSH_INTERVAL 秒写一次 Redis
        if now - last_flush >= FLUSH_INTERVAL:
            last_flush = now
            try:
                result = []
                # 按 count 降序取 TOP N
                sorted_chars = sorted(char_stats.items(),
                                      key=lambda x: x[1]["count"], reverse=True)[:TOP_N]

                for role_name, st in sorted_chars:
                    cnt = st["count"]
                    if cnt == 0:
                        continue

                    # 武器分布
                    total_w = sum(st["weapons"].values())
                    weapons_list = []
                    for wname, wcnt in sorted(st["weapons"].items(),
                                               key=lambda x: -x[1]):
                        weapons_list.append({
                            "name": wname,
                            "count": wcnt,
                            "ratio": round(wcnt / total_w, 2) if total_w > 0 else 0,
                            "icon": st["weapon_icons"].get(wname, "")
                        })

                    # 圣遗物分布
                    total_a = sum(st["artifacts"].values())
                    artifacts_list = []
                    for aname, acnt in sorted(st["artifacts"].items(),
                                               key=lambda x: -x[1]):
                        artifacts_list.append({
                            "name": aname,
                            "count": acnt,
                            "ratio": round(acnt / total_a, 2) if total_a > 0 else 0,
                            "icon": st["arti_icons"].get(aname, "")
                        })

                    result.append({
                        "role": role_name,
                        "star": st["star"],
                        "count": cnt,
                        "avg_constellation": round(st["constellation_sum"] / cnt, 1),
                        "avg_damage": int(st["damage_sum"] / cnt),
                        "weapons": weapons_list,
                        "artifacts": artifacts_list,
                        "avatar": st["avatar"],
                        "ename": st["ename"],
                    })

                r.set("build:hot_chars", json.dumps(result, ensure_ascii=False))
                names = ", ".join(f"{c['role']}({c['count']})" for c in result)
                print(f"[HotChar] 刷新 TOP{len(result)}: {names}")
            except Exception as e:
                print(f"[HotChar] ERROR: {e}")

            # ---- MySQL 落库（首次立即写，之后每 2 小时） ----
            should_flush_mysql = (not first_mysql_done) or (now - last_mysql_flush >= MYSQL_INTERVAL)
            if should_flush_mysql:
                last_mysql_flush = now
                try:
                    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,
                                           password=MYSQL_PASS, database=MYSQL_DB,
                                           charset="utf8mb4")
                    c = conn.cursor()
                    # 清理过期数据
                    c.execute("DELETE FROM rt_build_hot_char_detail WHERE snapshot_time < NOW() - INTERVAL %s SECOND", (MYSQL_RETENTION,))
                    # 写入当前窗口所有角色（不只是 TOP N）
                    snapshot_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
                    sql = """INSERT INTO rt_build_hot_char_detail
                             (snapshot_time, role_name, star, count, avg_constellation, avg_damage,
                              top_weapon, top_weapon_icon, top_weapon_ratio,
                              top_artifact, top_artifact_icon, top_artifact_ratio, avatar)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    total = 0
                    for role_name, st in char_stats.items():
                        cnt = st["count"]
                        if cnt == 0: continue
                        total_w = sum(st["weapons"].values())
                        top_w = sorted(st["weapons"].items(), key=lambda x: -x[1])[0] if st["weapons"] else ("?", 0)
                        total_a = sum(st["artifacts"].values())
                        top_a = sorted(st["artifacts"].items(), key=lambda x: -x[1])[0] if st["artifacts"] else ("?", 0)
                        c.execute(sql, (
                            snapshot_time, role_name, st["star"], cnt,
                            round(st["constellation_sum"] / cnt, 1),
                            int(st["damage_sum"] / cnt),
                            top_w[0],
                            st["weapon_icons"].get(top_w[0], ""),
                            round(top_w[1] / total_w, 2) if total_w > 0 else 0,
                            top_a[0],
                            st["arti_icons"].get(top_a[0], ""),
                            round(top_a[1] / total_a, 2) if total_a > 0 else 0,
                            st["avatar"]
                        ))
                        total += 1
                    conn.commit()
                    c.close()
                    conn.close()
                    first_mysql_done = True
                    print(f"[HotChar] MySQL落库: {total}角色, 清理过期")
                except Exception as e:
                    print(f"[HotChar] MySQL ERROR: {e}")


if __name__ == "__main__":
    main()
