#!/usr/bin/env python3
"""
逐风数据洞察平台 — 实时消费 Kafka → Redis
替代 Spark Streaming（避开 YARN executor 频繁掉线问题）
直接消费 Kafka 消息，滑动窗口聚合后写入 Redis。
"""
import json
import time
import threading
from collections import defaultdict, deque

try:
    from kafka import KafkaConsumer
    import redis
except ImportError:
    print("需要: pip install kafka-python redis")
    exit(1)

# ==================== 配置 ====================
KAFKA_BOOTSTRAP = "Middleware:9092"
REDIS_HOST = "Middleware"
REDIS_PORT = 6379
WINDOW_SECS = 1  # 滑动窗口秒数

# ==================== 消费者 ====================

def gacha_consumer():
    """消费 gacha-records（新版 Java 生成器）+ gacha_log（旧版），聚合写入 Redis gacha:*"""
    consumer = KafkaConsumer(
        "gacha-v2",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,   # 不提交偏移，每次重启自动从最新开始
        group_id="python-gacha-v9"
    )
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    window = deque()
    # 当期卡池物品动态累积计数（只增不减）
    banner_accum = defaultdict(int)
    print("[Gacha] 消费者启动 (gacha-records, Java Generator), 窗口=%ds" % WINDOW_SECS)

    for msg in consumer:
        now = time.time()
        data = msg.value
        window.append((now, data))

        while window and window[0][0] < now - WINDOW_SECS:
            window.popleft()

        if not window:
            continue

        pull_count = len(window)
        # 新格式: type="role"/"weapon" 默认5星（rate-up池）; 旧格式: star字段
        five_star_count = sum(1 for _, d in window
            if d.get("star") == 5 or d.get("type") in ("role", "weapon"))
        five_rate = round(five_star_count / pull_count * 100, 1) if pull_count > 0 else 0

        char_count = defaultdict(int)
        for _, d in window:
            char_count[d.get("item", "?")] += 1
        top_char = max(char_count.items(), key=lambda x: x[1])[0] if char_count else "?"

        # 物品分布（TOP50）
        items_top = sorted(char_count.items(), key=lambda x: -x[1])[:50]
        items_json = json.dumps([{"name": n, "count": c} for n, c in items_top], ensure_ascii=False)

        # 当期卡池物品单独统计（累积，只增不减）
        item = data.get("item", "?")
        banner_accum[item] += 1
        banner_json = json.dumps([{"name": k, "count": v} for k, v in banner_accum.items()], ensure_ascii=False)

        pipe = r.pipeline()
        pipe.set("gacha:pull_count", str(pull_count))
        pipe.set("gacha:five_star", str(five_rate))
        pipe.set("gacha:top_char", top_char)
        pipe.set("gacha:items", items_json)
        pipe.set("gacha:banner", banner_json)
        pipe.execute()
        print(f"[Gacha] pulls={pull_count} 5★={five_rate}% top={top_char}  ", end="\r")


def build_consumer():
    """消费 character-builds（新版Java） + build_log（旧版），聚合写入 Redis build:*"""
    consumer = KafkaConsumer(
        "build-v2",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,   # 不提交偏移，每次重启自动从最新开始
        group_id="python-build-v9"
    )
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    window = deque()
    print("[Build] 消费者启动 (character-builds + build_log), 窗口=%ds" % WINDOW_SECS)

    def get_weapon(d):
        w = d.get("weapon", "?")
        return w.get("name", "?") if isinstance(w, dict) else w
    def get_arti(d):
        a = d.get("artifact_set", d.get("artifact", "?"))
        return a.get("name", a.get("set_name", "?")) if isinstance(a, dict) else str(a) if a else "?"
    def get_dmg(d):
        return d.get("avg_damage", d.get("damage", 0))

    for msg in consumer:
        now = time.time()
        data = msg.value
        window.append((now, data))

        while window and window[0][0] < now - WINDOW_SECS:
            window.popleft()

        if not window:
            continue

        pull_count = len(window)

        # 只推最新到达的一条记录（data = 当前 msg.value）
        d = data
        rec = {
            "role": d.get("role", "?"),
            "star": d.get("star", 4),
            "constellation": d.get("constellation", 0),
            "level": d.get("level", 1),
            "damage": get_dmg(d),
            "weapon": get_weapon(d),
            "arti": get_arti(d)
        }
        r.lpush("build:recent", json.dumps(rec, ensure_ascii=False))
        r.ltrim("build:recent", 0, 19)  # 只保留最近20条

        print(f"[Build] pulls={pull_count}  ", end="\r")


if __name__ == "__main__":
    t1 = threading.Thread(target=gacha_consumer, daemon=True)
    t2 = threading.Thread(target=build_consumer, daemon=True)
    t1.start()
    t2.start()
    print("实时消费启动! Ctrl+C 停止")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n停止")
