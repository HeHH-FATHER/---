#!/usr/bin/env python3
"""
大数据链路监控采集器 — Middleware 侧（每5秒采集 → Redis screen:monitor）
master0 指标由 master0 上的 monitor_master0.py 配套写入同一 Redis Hash
采集维度：消息队列 | 存储 | 系统资源 | 消费端
"""
import time, os, subprocess, sys
try: import redis
except ImportError:
    print("安装 redis...")
    os.system("pip3 install redis 2>/dev/null")
    try: import redis
    except ImportError: print("FATAL: redis 安装失败"); sys.exit(1)

REDIS_HOST = os.environ.get("REDIS_HOST", "100.103.177.85")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
OLD_KEYS = ["kafka_lag", "spark_delay"]  # 旧版垃圾 key

# Kafka offset 缓存（GetOffsetShell 要启 JVM，太慢不能每轮调）
_kafka_cache = {"build-v2": -1, "satisfaction-v1": -1}
_kafka_cycle = 0
KAFKA_INTERVAL = 6  # 每 6 轮（30s）更新一次

# ═══════════════════════════════════════════
# 📨 消息队列（本地 Kafka）
# ═══════════════════════════════════════════

def get_kafka_offset(topic):
    try:
        result = subprocess.run(
            ["/root/kafka2/bin/kafka-run-class.sh", "kafka.tools.GetOffsetShell",
             "--broker-list", "localhost:9092", "--topic", topic, "--time", "-1"],
            capture_output=True, text=True, timeout=15)
        if result.returncode == 0 and ":" in result.stdout:
            return int(result.stdout.strip().split(":")[-1])
    except: pass
    return -1

def get_kafka_offsets():
    """每 KAFKA_INTERVAL 轮才真正调 GetOffsetShell"""
    global _kafka_cache, _kafka_cycle
    _kafka_cycle += 1
    if _kafka_cycle % KAFKA_INTERVAL == 1:
        for topic in ["build-v2", "satisfaction-v1"]:
            val = get_kafka_offset(topic)
            if val >= 0: _kafka_cache[topic] = val
    data = {}
    if _kafka_cache["build-v2"] >= 0: data["kafka_build_offset"] = _kafka_cache["build-v2"]
    if _kafka_cache["satisfaction-v1"] >= 0: data["kafka_sat_offset"] = _kafka_cache["satisfaction-v1"]
    return data

# ═══════════════════════════════════════════
# 🗄️ 存储（本地 Redis）
# ═══════════════════════════════════════════

def get_redis_metrics(r):
    try:
        info = r.info("stats")
        mem = r.info("memory")
        used = mem.get("used_memory", 0)
        maxmem = mem.get("maxmemory", 0) or 256 * 1024 * 1024
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 1)
        return {
            "redis_mem_mb": round(used / 1024 / 1024, 1),  # 绝对内存 MB
            "redis_ops": info.get("instantaneous_ops_per_sec", 0),
            "redis_hit_rate": round(hits / (hits + misses) * 100, 1) if (hits + misses) > 0 else 100,
            "redis_keys": r.dbsize(),
        }
    except: return {}

# ═══════════════════════════════════════════
# 💻 本机系统
# ═══════════════════════════════════════════

def get_local_system():
    try:
        with open("/proc/loadavg") as f:
            load = float(f.read().split()[0])
        with open("/proc/cpuinfo") as f:
            cores = sum(1 for line in f if line.startswith("processor"))
        cpu_pct = round(min(load / max(cores, 1) * 100, 100), 1)
        result = subprocess.run(["free", "-b"], capture_output=True, text=True, timeout=3)
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            total, used = int(parts[1]), int(parts[2])
            mem_pct = round(used / total * 100, 1) if total > 0 else 0
        else: mem_pct = 0
        result = subprocess.run(["df", "-B1", "/"], capture_output=True, text=True, timeout=3)
        parts = result.stdout.strip().split("\n")[-1].split()
        if len(parts) >= 5:
            disk_pct = round(int(parts[2]) / int(parts[1]) * 100, 1)
        else: disk_pct = 0
        return {"mw_cpu": cpu_pct, "mw_mem": mem_pct, "mw_disk": disk_pct}
    except: return {}

def get_consumer_count():
    """Python 消费端进程数（排除自身）"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=3)
        count = sum(1 for line in result.stdout.split("\n")
                    if "python3" in line and "monitor_collector" not in line
                    and any(k in line for k in
                       ["realtime_consumer", "hot_char_aggregator", "satisfaction_consumer"]))
        return count
    except: return 0

# ═══════════════════════════════════════════
# 主循环
# ═══════════════════════════════════════════

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=3)
    loop = "--loop" in sys.argv

    print("[Monitor-MW] 采集器启动（Kafka 每30s / 其他每5s）")
    while True:
        data = {}

        # 消息队列（缓存模式，不会阻塞主循环）
        data.update(get_kafka_offsets())

        # 存储
        data.update(get_redis_metrics(r))

        # 本机系统
        data.update(get_local_system())

        # 消费端
        data["consumer_count"] = get_consumer_count()

        # 写入 Redis
        if data:
            r.hmset("screen:monitor", data)  # Redis 3.0 兼容，不用 hset mapping=
            r.expire("screen:monitor", 300)
            r.hdel("screen:monitor", *OLD_KEYS)

        print(f"[Monitor-MW] Redis-ops={data.get('redis_ops',0)} MW-CPU={data.get('mw_cpu',0)} Consumers={data.get('consumer_count',0)}")

        if not loop: break
        time.sleep(5)

if __name__ == "__main__":
    main()
