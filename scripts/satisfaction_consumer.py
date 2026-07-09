#!/usr/bin/env python3
"""消费 Kafka satisfaction-v1 → Redis"""
import json, sys
from kafka import KafkaConsumer
import redis

BOOTSTRAP = "Middleware:9092"
TOPIC = "satisfaction-v1"
REDIS_HOST = "Middleware"

consumer = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest", enable_auto_commit=False,
    group_id="python-satisfaction-v1")
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
pipe = r.pipeline()
prev = {}
cache = {}  # {role: {star, ability, look}} — 缓存角色属性

print("[Satis] Consumer started")

for msg in consumer:
    data = msg.value
    role = data.get("role", "?")
    satify = data.get("satify", 5)
    cache[role] = {"star": data.get("star", 5), "ability": data.get("ability", satify), "look": data.get("look", satify)}
    pipe.zadd("rt:satisfaction:ranking", {role: satify})
    pipe.rpush(f"rt:satisfaction:trend:{role}", str(satify))
    pipe.ltrim(f"rt:satisfaction:trend:{role}", -60, -1)
    pipe.execute()

    # TOP6 快照（简化，取 ZSET 前 6）
    top = r.zrevrange("rt:satisfaction:ranking", 0, 5, withscores=True)
    top_list = []
    for t in top:
        role = t[0]
        d = {"role": role, "satify": t[1], "delta": 0,
             "star": cache.get(role, {}).get("star", 5),
             "ability": cache.get(role, {}).get("ability", t[1]),
             "look": cache.get(role, {}).get("look", t[1])}
        if role in prev:
            d["delta"] = round(t[1] - prev[role], 1)
        prev[role] = t[1]
        trend_vals = r.lrange(f"rt:satisfaction:trend:{role}", -22, -1)
        d["trend"] = [float(v) for v in trend_vals]
        top_list.append(d)
    r.set("satisfaction:top", json.dumps(top_list, ensure_ascii=False))
