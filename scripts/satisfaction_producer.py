#!/usr/bin/env python3
"""
逐风数据洞察平台 — 实时链路④ 满意度投票生成器
==================================================
模拟玩家持续给角色打分的实时流。
每 3 秒随机产生一批新投票 → 更新滚动平均分 → 写入 Redis satisfaction:top

用法:
  python3 satisfaction_producer.py              # 单次
  python3 satisfaction_producer.py --loop       # 持续运行
"""
import json, time, random, sys, os

try: import redis
except ImportError: print("需要 pip install redis"); sys.exit(1)

REDIS_HOST = os.environ.get("REDIS_HOST", "100.103.177.85")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "提瓦特数据")
FLUSH_INTERVAL = 3  # 每 3 秒刷新一次

class SatisfactionSimulator:
    """模拟实时满意度投票：每次随机选几个角色，产生新评分，更新滚动平均"""

    def __init__(self):
        path = os.path.join(DATA_DIR, "角色满意度排行.json")
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # 初始化状态：每角色累计评分和投票数
        self.chars = {}
        for d in raw:
            v = max(d.get("vote_sum", 1), 1)
            a = max(d.get("avg_satify", 5), 1)
            b = max(d.get("avg_ability", 5), 1)
            c_val = max(d.get("avg_look", 5), 1)
            self.chars[d["role"]] = {
                "star": d.get("star", 4),
                "vote_sum": v,
                "satify_sum": a * v,
                "ability_sum": b * v,
                "look_sum": c_val * v,
            }
        self.rng = random.Random()

    def tick(self, batch_size=5000):
        """模拟舆论波动：每轮随机数量、随机幅度"""
        names = list(self.chars.keys())
        self.rng.shuffle(names)
        # 随机 5~40 个角色
        n = self.rng.randint(5, 40)
        for name in names[:n]:
            c = self.chars[name]
            avg = c["satify_sum"] / c["vote_sum"]
            # 30%概率大幅波动(±0.5~1.0)，70%小幅(±0.05~0.2)
            if self.rng.random() < 0.3:
                delta = self.rng.uniform(-1.0, 1.0)
            else:
                delta = self.rng.uniform(-0.2, 0.2)
            new_avg = round(max(1.0, min(10.0, avg + delta)), 1)
            c["satify_sum"] = new_avg * c["vote_sum"]
            c["ability_sum"] = min(10.0, new_avg + self.rng.uniform(-0.3, 0.3)) * c["vote_sum"]
            c["look_sum"] = min(10.0, new_avg + self.rng.uniform(-0.3, 0.3)) * c["vote_sum"]

    def get_top(self, n=8):
        """返回当前满意度 TOP N"""
        result = []
        for name, c in self.chars.items():
            result.append({
                "role": name,
                "star": c["star"],
                "satify": round(c["satify_sum"] / c["vote_sum"], 1),
                "ability": round(c["ability_sum"] / c["vote_sum"], 1),
                "look": round(c["look_sum"] / c["vote_sum"], 1),
                "vote_sum": c["vote_sum"],
            })
        result.sort(key=lambda x: -x["satify"])
        return result[:n]


# 上一轮分数缓存(用于计算 delta)
_prev_scores = {}

def flush_to_redis(sim, r):
    """每轮写入 Redis:
    1. ZSET: rt:satisfaction:ranking — 所有角色按满意度排序
    2. LIST: rt:satisfaction:trend:{name} — 每角色近60轮趋势
    3. STRING: satisfaction:top — TOP8 JSON 快照(含 delta)
    """
    global _prev_scores
    all_chars = sim.get_top(len(sim.chars))
    pipe = r.pipeline()

    # ZSET: 全量排名，按 satify 分排序
    for c in all_chars:
        pipe.zadd("rt:satisfaction:ranking", {c["role"]: c["satify"]})

    # LIST: 每角色趋势线(保留最近 60 个点)
    for c in all_chars:
        key = f"rt:satisfaction:trend:{c['role']}"
        pipe.rpush(key, str(c["satify"]))
        pipe.ltrim(key, -60, -1)

    # STRING: TOP8 快照(含 delta + 趋势数组)
    top6 = all_chars[:6]
    for c in top6:
        prev = _prev_scores.get(c["role"])
        c["delta"] = round(c["satify"] - prev, 1) if prev is not None else 0
        _prev_scores[c["role"]] = c["satify"]
        # 取最近 20 个趋势值(来自 last list push)
        trend_key = f"rt:satisfaction:trend:{c['role']}"
        trend_vals = r.lrange(trend_key, -20, -1)
        c["trend"] = [float(v) for v in trend_vals] if trend_vals else []
    pipe.set("satisfaction:top", json.dumps(top6, ensure_ascii=False))

    pipe.execute()
    return all_chars[:6]


def main():
    loop = "--loop" in sys.argv
    sim = SatisfactionSimulator()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5)

    print(f"[Satisfaction] ZSET + LIST 模式启动, 每{FLUSH_INTERVAL}s刷新")
    while True:
        sim.tick(batch_size=5000)
        top6 = flush_to_redis(sim, r)
        names = ", ".join(f"{c['role']}({c['satify']})" for c in top6[:3])
        print(f"[Satisfaction] ZSET={r.zcard('rt:satisfaction:ranking')}角色, TOP4: {names}")
        if not loop: break
        time.sleep(FLUSH_INTERVAL)


if __name__ == "__main__":
    main()
