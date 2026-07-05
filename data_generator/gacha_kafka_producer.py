#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 抽卡记录 Kafka Producer
从 卡池统计_clean.json 读取卡池数据，模拟抽卡记录实时写入 Kafka topic gacha_log

依赖: kafka-python, gacha_common
用法:
  python3 gacha_kafka_producer.py                    # 默认 500条/秒
  python3 gacha_kafka_producer.py --rate 1000        # 1000条/秒
  python3 gacha_kafka_producer.py --count 100000     # 10万条后退出
"""
import json
import sys
import time
from kafka import KafkaProducer
from gacha_common import load_banners, generate_record

# ==================== 配置 ====================
BROKER = "Middleware:9092"
TOPIC = "gacha_log"
RATE_PER_SEC = 500
MAX_COUNT = 0  # 0 = 无限


def main():
    rate = RATE_PER_SEC
    max_count = MAX_COUNT
    for i, arg in enumerate(sys.argv):
        if arg == "--rate" and i + 1 < len(sys.argv):
            rate = int(sys.argv[i + 1])
        if arg == "--count" and i + 1 < len(sys.argv):
            max_count = int(sys.argv[i + 1])

    banners = load_banners()
    print(f"加载 {len(banners)} 期卡池, 连接 Kafka: {BROKER} → {TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=0,
        compression_type="gzip",
        batch_size=16384,
        linger_ms=10,
        max_in_flight_requests_per_connection=5
    )

    pity_state = {}
    count = 0
    five_star_count = 0
    start_time = time.time()

    print(f"速率: {rate} 条/秒, 总条数: {'无限' if max_count == 0 else f'{max_count:,}'}, Ctrl+C 停止\n")

    try:
        while True:
            if max_count > 0 and count >= max_count:
                break

            record, pity_state = generate_record(banners, pity_state)
            producer.send(TOPIC, value=record)
            count += 1
            if record["star"] == 5:
                five_star_count += 1

            if count % (rate * 5) == 0:
                elapsed = time.time() - start_time
                real_rate = count / elapsed if elapsed > 0 else 0
                five_pct = five_star_count / count * 100 if count > 0 else 0
                print(f"  已发送 {count:,} 条 | 速率 {real_rate:.0f} 条/秒 | 五星率 {five_pct:.1f}%")

            if count % rate == 0:
                time.sleep(0.9)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - start_time
    real_rate = count / elapsed if elapsed > 0 else 0
    print(f"\n=== 统计 ===")
    print(f"  总发送: {count:,} 条 | 五星: {five_star_count} | 速率: {real_rate:.0f} 条/秒")


if __name__ == "__main__":
    main()
