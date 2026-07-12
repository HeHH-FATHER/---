#!/usr/bin/env python3
"""
master0 系统指标采集器 — 每5秒采集本地+slave指标 → Redis screen:monitor (Middleware)
采集维度：HDFS | 所有节点系统资源 | 生成器进程
"""
import time, os, subprocess, sys, json
try: import redis
except ImportError:
    print("安装 redis...")
    os.system("pip3 install redis 2>/dev/null")
    try: import redis
    except ImportError: print("FATAL: redis 安装失败"); sys.exit(1)

REDIS_HOST = "Middleware"
REDIS_PORT = 6379
# 所有需要监控的节点
NODES = {"master0": "localhost", "slave1": "slave1", "slave2": "slave2", "backup": "backup", "vserver": "vserver"}

def ssh(host, cmd, timeout=5):
    """SSH 执行命令，返回 stdout"""
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=3",
             host, cmd], capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip() if result.returncode == 0 else ""
    except: return ""

def get_hdfs_usage():
    """HDFS 使用率（通过 NameNode JMX API，避免 dfsadmin 启 JVM 吃 CPU）"""
    try:
        result = subprocess.run(
            ["curl", "-s", "--connect-timeout", "3", "http://localhost:50070/jmx"],
            capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout:
            data = json.loads(result.stdout)
            for bean in data.get("beans", []):
                if bean.get("name") == "Hadoop:service=NameNode,name=FSNamesystem":
                    used = bean.get("CapacityUsed", 0)
                    total = bean.get("CapacityTotal", 1)
                    return int(used / total * 100) if total > 0 else -1
    except: pass
    return -1

def get_node_metrics(host):
    """收集单节点 CPU/内存/磁盘，返回 {"cpu": x, "mem": y, "disk": z}
    CPU: 真实使用率 %（loadavg / nproc * 100）"""
    cpu_str = ssh(host, "cat /proc/loadavg | awk '{print $1}'")
    cores_str = ssh(host, "nproc")
    cpu_load = float(cpu_str) if cpu_str else 0
    cores = int(cores_str) if cores_str else 1
    cpu_pct = round(min(cpu_load / cores * 100, 100), 1)  # loadavg → 百分比

    mem_str = ssh(host, "free -b | awk 'NR==2{printf \"%.1f\", $3/$2*100}'")
    mem = float(mem_str) if mem_str else 0

    disk_str = ssh(host, "df -B1 / | awk 'NR==2{printf \"%.1f\", $3/$2*100}'")
    disk = float(disk_str) if disk_str else 0

    return {"cpu": cpu_pct, "mem": mem, "disk": disk}

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

    print("[Monitor-M0] 采集器启动（本地每5s / 远程每15s）")
    _cycle = 0
    while True:
        _cycle += 1
        data = {}

        # HDFS + 生成器 + Spark（本地，每轮）
        hdfs = get_hdfs_usage()
        if hdfs >= 0: data["hdfs_usage"] = hdfs
        data["generator_count"] = get_generator_count()
        data["streaming_count"] = get_spark_streaming_count()
        data.update(get_spark_metrics())

        # 远程节点：每 3 轮（15s）SSH 一次
        if _cycle % 3 == 1:
            for name, host in NODES.items():
                if name == "master0": continue  # 本地直接取
                m = get_node_metrics(host)
                data[f"{name}_cpu"] = m["cpu"]
                data[f"{name}_mem"] = m["mem"]
                data[f"{name}_disk"] = m["disk"]
            # 本地 master0
            m = get_node_metrics("localhost")
            data["m0_cpu"] = m["cpu"]
            data["m0_mem"] = m["mem"]
            data["m0_disk"] = m["disk"]

        try:
            r.hmset("screen:monitor", data)
            r.expire("screen:monitor", 300)
        except Exception as e:
            print(f"[Monitor-M0] Redis 写入失败: {e}")

        remote = "/".join([f"{n}={data.get(n+'_cpu',data.get('m0_cpu' if n=='master0' else n+'_cpu',0))}" for n in ["master0","slave1","slave2","backup","vserver"]])
        print(f"[Monitor-M0] HDFS={data.get('hdfs_usage','?')}% Gen={data.get('generator_count',0)} Spark={data.get('streaming_count',0)} | {remote}")

        if not loop: break
        time.sleep(5)

if __name__ == "__main__":
    main()
