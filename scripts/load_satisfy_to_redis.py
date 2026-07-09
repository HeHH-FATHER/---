#!/usr/bin/env python3
"""
逐风数据洞察平台 — 满意度数据加载（Redis）
读取 角色满意度排行.json → 写入 Redis satify:top
"""
import json, os, sys

try: import redis
except ImportError: print("需要 pip install redis"); sys.exit(1)

HOST = os.environ.get("REDIS_HOST", "100.103.177.85")
PORT = int(os.environ.get("REDIS_PORT", 6379))
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "提瓦特数据")

def main():
    path = os.path.join(DATA_DIR, "角色满意度排行.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 按 avg_satify 降序取 TOP 8
    data.sort(key=lambda x: x.get("avg_satify", 0), reverse=True)
    top8 = []
    for d in data[:8]:
        top8.append({
            "role": d["role"],
            "star": d["star"],
            "satify": d["avg_satify"],
            "ability": d["avg_ability"],
            "look": d["avg_look"],
            "vote_sum": d["vote_sum"],
        })

    r = redis.Redis(host=HOST, port=PORT, decode_responses=True, socket_connect_timeout=5)
    r.set("satisfaction:top", json.dumps(top8, ensure_ascii=False))
    print(f"[Satisfaction] 写入 Redis satisfaction:top → TOP{len(top8)}: {', '.join(d['role'] for d in top8)}")

if __name__ == "__main__":
    import time
    loop = "--loop" in sys.argv
    interval = int(sys.argv[sys.argv.index("--loop")+1]) if loop and len(sys.argv) > sys.argv.index("--loop")+1 else 0
    while True:
        main()
        if not loop: break
        time.sleep(interval)
