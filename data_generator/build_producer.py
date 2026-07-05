#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 练度记录生成器（实时链②）
从 角色练度统计.json 读取角色练度分布，模拟单条用户练度记录。

输出格式（每条记录 = 单个角色的一条随机练度快照）:
  {"uid":"100000042","char":"玛薇卡","star":5,"constellation":2,
   "level":90,"weapon":"焚曜千阳","artifact":"黑曜秘典4",
   "damage":118748,"timestamp":1751401234567}

用法:
  python build_producer.py                  # 默认 10000 条 → 文件
  python build_producer.py --count 50000    # 5 万条 → 文件
  python build_producer.py kafka            # 持续写入 Kafka
  python build_producer.py kafka --rate 200  # Kafka 200 条/秒
"""

import json
import os
import sys
import time
import random
from datetime import datetime

# ==================== 配置 ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "提瓦特数据")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
DEFAULT_COUNT = 10000
KAFKA_BROKER = "Middleware:9092"
KAFKA_TOPIC = "build_log"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==================== 加载数据源 ====================
def load_data():
    path = os.path.join(DATA_DIR, "角色练度统计.json")
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    chars = []
    for r in raw:
        # 只保留至少有 weapon 和 artifact 数据的角色
        if not r.get("weapons") or not r.get("artifact_sets"):
            continue
        chars.append({
            "name": r["role"],
            "star": r["star"],
            "avg_damage": r.get("avg_damage", 1000),
            "const_dist": r.get("constellation_dist", {}),
            "weapons": r["weapons"],
            "artifacts": r["artifact_sets"],
        })
    return chars

# ==================== 生成逻辑 ====================

def pick_constellation(const_dist):
    """按 c0~c6 分布概率随机选命座"""
    keys = [f"c{i}" for i in range(7)]
    probs = [const_dist.get(k, 0) for k in keys]
    total = sum(probs)
    if total <= 0:
        return random.randint(0, 6)
    # 归一化
    probs = [p / total for p in probs]
    r = random.random()
    cum = 0
    for i, p in enumerate(probs):
        cum += p
        if r < cum:
            return i
    return 6

def pick_from_rate(items, name_key="name"):
    """按 rate 加权随机选"""
    if not items:
        return ""
    names = [x[name_key] for x in items]
    rates = [x.get("rate", 0) for x in items]
    total = sum(rates)
    if total <= 0:
        return names[0]
    r = random.random() * total
    cum = 0
    for i, rate in enumerate(rates):
        cum += rate
        if r < cum:
            return names[i]
    return names[-1]

def generate_record(uid, chars):
    """为单个用户生成一条练度记录"""
    c = random.choice(chars)
    cons = pick_constellation(c["const_dist"])
    weapon = pick_from_rate(c["weapons"])
    artifact = pick_from_rate(c["artifacts"])
    # 伤害按均值的 0.5~2.0 倍波动
    base_dmg = c["avg_damage"]
    dmg = int(base_dmg * random.uniform(0.5, 2.0))
    ts = int(time.time() * 1000)

    return {
        "uid": str(uid),
        "char": c["name"],
        "star": c["star"],
        "constellation": cons,
        "level": 90 if c["star"] == 5 else 80,
        "weapon": weapon,
        "artifact": artifact,
        "damage": dmg,
        "timestamp": ts,
    }

# ==================== 文件输出 ====================
def generate_to_file(count):
    chars = load_data()
    print(f"加载 {len(chars)} 个角色练度数据")
    print(f"生成 {count:,} 条练度记录...")

    uid_base = 100_000_000
    records = []
    for i in range(count):
        uid = uid_base + (i % 10000)
        records.append(generate_record(uid, chars))

    path = os.path.join(OUTPUT_DIR, "build_records.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"完成! → {path}")

# ==================== Kafka 输出 ====================
def generate_to_kafka(rate=200):
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[FATAL] pip install kafka-python")
        sys.exit(1)

    chars = load_data()
    print(f"加载 {len(chars)} 个角色练度数据")
    print(f"连接 Kafka: {KAFKA_BROKER} → {KAFKA_TOPIC} ({rate}条/秒)")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=0,
        compression_type="gzip",
    )

    uid_base = 100_000_000
    count = 0
    try:
        while True:
            uid = uid_base + (count % 10000)
            rec = generate_record(uid, chars)
            producer.send(KAFKA_TOPIC, value=rec)
            count += 1
            if count % 100 == 0:
                producer.flush()
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print(f"\n停止。共发送 {count} 条")
    finally:
        producer.close()

# ==================== CLI ====================
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "kafka":
        rate = 200
        for i, a in enumerate(sys.argv):
            if a == "--rate" and i + 1 < len(sys.argv):
                rate = int(sys.argv[i + 1])
        generate_to_kafka(rate)
    else:
        count = DEFAULT_COUNT
        for i, a in enumerate(sys.argv):
            if a == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])
        generate_to_file(count)
