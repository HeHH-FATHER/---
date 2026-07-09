#!/usr/bin/env python3
"""
master0 系统指标采集器 — 每5秒采集本地+slave指标 → Redis screen:monitor (Middleware)
采集维度：HDFS | 所有节点系统资源 | 生成器进程
"""
import time, os, subprocess, sys
try: import redis
except ImportError: print("需要 pip install redis"); sys.exit(1)

REDIS_HOST = "Middleware"
REDIS_PORT = 6379
# 所有需要监控的节点
NODES = {"master0": "localhost", "slave1": "slave1", "slave2": "slave2", "backup": "backup"}

def ssh(host, cmd, timeout=5):
    """SSH 执行命令，返回 stdout"""
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=3",
             host, cmd], capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip() if result.returncode == 0 else ""
    except: return ""

def get_hdfs_usage():
    try:
        result = subprocess.run(
            "/root/hadoop-2.7.6/bin/hdfs dfsadmin -report 2>/dev/null | grep 'DFS Used%' | head -1 | awk '{print $3}' | tr -d '%'",
            shell=True, capture_output=True, text=True, timeout=5)
        val = result.stdout.strip()
        return int(float(val)) if val else -1
    except: return -1

def get_node_metrics(host):
    """收集单节点 CPU/内存/磁盘，返回 {"cpu": x, "mem": y, "disk": z}"""
    cpu_str = ssh(host, "cat /proc/loadavg | awk '{print $1}'")
    cpu = round(float(cpu_str), 1) if cpu_str else 0

    mem_str = ssh(host, "free -b | awk 'NR==2{printf \"%.1f\", $3/$2*100}'")
    mem = float(mem_str) if mem_str else 0

    disk_str = ssh(host, "df -B1 / | awk 'NR==2{printf \"%.1f\", $3/$2*100}'")
    disk = float(disk_str) if disk_str else 0

    return {"cpu": cpu, "mem": mem, "disk": disk}

def get_spark_metrics():
    """Spark Master REST API 指标"""
    try:
        result = subprocess.run(
            ["curl", "-s", "http://localhost:8080/json/"],
            capture_output=True, text=True, timeout=5)
        if result.returncode != 0: return {}
        j = __import__('json').loads(result.stdout)
        workers = j.get("workers", [])
        alive = sum(1 for w in workers if w.get("state") == "ALIVE")
        cores_used = sum(w.get("coresused", 0) for w in workers)
        cores_total = sum(w.get("cores", 0) for w in workers)
        mem_used = sum(w.get("memoryused", 0) for w in workers)
        mem_total = sum(w.get("memory", 0) for w in workers)
        apps = len(j.get("activeapps", []))
        return {
            "spark_workers": alive,
            "spark_cores_used": cores_used, "spark_cores_total": cores_total,
            "spark_mem_used": mem_used, "spark_mem_total": mem_total,
            "spark_apps": apps,
            "spark_status": j.get("status", "UNKNOWN"),
        }
    except: return {}

def get_generator_count():
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=3)
        return sum(1 for line in result.stdout.split("\n")
                   if "java" in line and any(k in line for k in ["build_stats", "Satisfaction"]))
    except: return 0

def get_spark_streaming_count():
    """Spark Streaming 作业数（SparkSubmit 进程）"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=3)
        return sum(1 for line in result.stdout.split("\n")
                   if "SparkSubmit" in line and not "grep" in line)
    except: return 0

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=3)
    loop = "--loop" in sys.argv

    print("[Monitor-M0] 采集器启动（4节点 + HDFS + 生成器）")
    while True:
        data = {}

        # HDFS
        hdfs = get_hdfs_usage()
        if hdfs >= 0: data["hdfs_usage"] = hdfs

        # 生成器
        data["generator_count"] = get_generator_count()

        # Spark Streaming
        data["streaming_count"] = get_spark_streaming_count()

        # Spark
        data.update(get_spark_metrics())

        # 所有节点系统指标（SSH 到 localhost/slave1/slave2/backup）
        for name, host in NODES.items():
            m = get_node_metrics(host)
            prefix = "m0" if name == "master0" else name
            data[f"{prefix}_cpu"] = m["cpu"]
            data[f"{prefix}_mem"] = m["mem"]
            data[f"{prefix}_disk"] = m["disk"]

        try:
            r.hmset("screen:monitor", data)
            r.expire("screen:monitor", 300)
        except Exception as e:
            print(f"[Monitor-M0] Redis 写入失败: {e}")

        cpu_mem = []
        for name in ["master0","slave1","slave2","backup"]:
            k = "m0" if name == "master0" else name
            cpu_mem.append(f"{name}={data.get(f'{k}_cpu',0)}/{data.get(f'{k}_mem',0)}%")
        print(f"[Monitor-M0] HDFS={data.get('hdfs_usage','?')}% Gen={data['generator_count']} | " + " | ".join(cpu_mem))

        if not loop: break
        time.sleep(5)

if __name__ == "__main__":
    main()
