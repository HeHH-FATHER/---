#!/usr/bin/env python3
"""
逐风数据洞察平台 — 管道仿真器（集群离线时使用 · 持续运行版）
============================================================
完全平替 run_gen_loop.sh + realtime_consumer.py + hot_char_aggregator.py。
匹配生成器速率: Build 10条/秒, Gacha 4条/秒。
滑动窗口: Gacha/Build 1秒, HotChar 60秒。
MySQL 落库: 每2小时。

明天集群恢复 → 停此脚本 → 启动正常管道 → 效果无缝切换。
"""
import json, time, random, sys, os, threading
from collections import defaultdict, deque

# ==================== 配置 ====================
REDIS_HOST = os.environ.get("REDIS_HOST", "100.103.177.85")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
MYSQL_HOST = os.environ.get("MYSQL_HOST", "100.103.177.85")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "123456")
MYSQL_DB   = os.environ.get("MYSQL_DB", "abyss_db")

BUILD_PER_SEC = 10      # 练度 每秒10条
GACHA_PER_SEC = 4       # 抽卡 每秒4条
GACHA_WINDOW = 1        # 抽卡滑动窗口 1秒
BUILD_WINDOW = 1        # 练度滑动窗口 1秒
HOTCHAR_WINDOW = 60     # 热门角色窗口 60秒
HOTCHAR_FLUSH = 3       # Redis 刷新间隔 3秒
MYSQL_INTERVAL = 30   # MySQL 落库 2小时
MYSQL_RETENTION = 43200 # MySQL 保留 12小时

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "提瓦特数据")

try:
    import redis, pymysql
except ImportError:
    print("需要: pip install redis pymysql"); sys.exit(1)


def load_build_source():
    path = os.path.join(DATA_DIR, "角色练度统计.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_gacha_source():
    path = os.path.join(DATA_DIR, "卡池统计_clean.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ==================== 生成器（平替 Java Generator） ====================

def gen_build_record(build_data):
    """按真实数据分布生成 1 条练度记录（平替 Character-Build-Generator）"""
    ch = random.choice(build_data)
    weapon = random.choice(ch["weapons"]) if ch["weapons"] else {"name": "?", "avatar": ""}
    arti = random.choice(ch["artifact_sets"]) if ch["artifact_sets"] else {"name": "?", "avatars": []}
    # 按真实命座分布采样
    con_dist = ch.get("constellation_dist", {})
    con = 0
    if con_dist:
        r = random.random()
        acc = 0
        for k, v in sorted(con_dist.items()):
            acc += v / 100.0 if v > 1 else v
            if r <= acc:
                try: con = int(k.replace("c","").replace("constellation_",""))
                except: con = 0
                break
    return {
        "role": ch["role"], "star": ch["star"],
        "constellation": con,
        "level": int(ch.get("avg_level", 80)),
        "avg_damage": int(ch.get("avg_damage", 0)),
        "weapon": {"name": weapon["name"], "avatar": weapon.get("avatar", "")},
        "artifact_set": {"name": arti["name"], "avatars": arti.get("avatars", [])},
        "avatar": ch.get("avatar", ""), "ename": ch.get("ename", ""),
    }

def gen_gacha_record(gacha_data):
    """按真实卡池数据生成 1 条抽卡记录（平替 Gacha-Record-Generator）"""
    all_items = []
    if isinstance(gacha_data, dict):
        for lst_name in ["roleList", "weaponList", "hybridList"]:
            for item in gacha_data.get(lst_name, []):
                content = item.get("content", {})
                for name, count in content.items():
                    ptype = "weapon" if lst_name == "weaponList" else "role"
                    all_items.append((name, ptype, max(count, 1)))
    if not all_items:
        all_items = [("?", "role", 1)]
    total = sum(w for _, _, w in all_items)
    r = random.uniform(0, total)
    acc = 0
    for name, ptype, w in all_items:
        acc += w
        if r <= acc:
            return {"item": name, "type": ptype, "star": 5, "banner": "6.6下半"}
    return {"item": "?", "type": "role", "star": 4, "banner": "6.6下半"}

# ==================== 消费端（平替 realtime_consumer.py） ====================

class PipelineSimulator:
    def __init__(self, build_data, gacha_data, r, conn):
        self.build_data = build_data
        self.gacha_data = gacha_data
        self.r = r
        self.conn = conn
        self.gacha_window = deque()
        self.build_window = deque()
        self.hotchar_window = deque()
        self.banner_accum = defaultdict(int)
        self.running = True

    def run_forever(self):
        i = 0
        last_hotchar_flush = time.time()
        last_mysql_flush = time.time()

        print(f"[Sim] 启动: Build={BUILD_PER_SEC}/s, Gacha={GACHA_PER_SEC}/s")
        print(f"[Sim] 窗口: Gacha/Build={GACHA_WINDOW}s, HotChar={HOTCHAR_WINDOW}s")
        print(f"[Sim] Redis 刷新: 每{HOTCHAR_FLUSH}s, MySQL: 每{MYSQL_INTERVAL}s")

        while self.running:
            now = time.time()
            # ---- 生成 Build 消息 (10条/秒) ----
            self._build_this_cycle = []
            for _ in range(BUILD_PER_SEC):
                rec = gen_build_record(self.build_data)
                self._build_this_cycle.append(rec)
                self.build_window.append((now, rec))
                self.hotchar_window.append((now, rec))
            # ---- 生成 Gacha 消息 (4条/秒) ----
            for _ in range(GACHA_PER_SEC):
                rec = gen_gacha_record(self.gacha_data)
                self.gacha_window.append((now, rec))
                self.banner_accum[rec.get("item", "?")] += 1

            # ---- 清理过期窗口 ----
            while self.gacha_window and self.gacha_window[0][0] < now - GACHA_WINDOW:
                self.gacha_window.popleft()
            while self.build_window and self.build_window[0][0] < now - BUILD_WINDOW:
                self.build_window.popleft()
            while self.hotchar_window and self.hotchar_window[0][0] < now - HOTCHAR_WINDOW:
                self.hotchar_window.popleft()

            # ---- Gacha → Redis (每轮都写，匹配 realtime_consumer) ----
            gw = self.gacha_window
            if gw:
                pc = len(gw)
                five = sum(1 for _, d in gw if d.get("star") == 5)
                cc = defaultdict(int)
                for _, d in gw: cc[d.get("item", "?")] += 1
                tc = max(cc.items(), key=lambda x: x[1])[0] if cc else "?"
                its = [{"name": n, "count": c} for n, c in sorted(cc.items(), key=lambda x: -x[1])[:50]]
                bnr = [{"name": k, "count": v} for k, v in sorted(self.banner_accum.items(), key=lambda x: -x[1])[:50]]
                pipe = self.r.pipeline()
                pipe.set("gacha:pull_count", str(pc))
                pipe.set("gacha:five_star", str(round(five/pc*100,1) if pc>0 else 0))
                pipe.set("gacha:top_char", tc)
                pipe.set("gacha:items", json.dumps(its, ensure_ascii=False))
                pipe.set("gacha:banner", json.dumps(bnr, ensure_ascii=False))
                pipe.execute()

            # ---- Build recent → Redis (逐条推送，匹配 realtime_consumer) ----
            # 只推本轮新生成的记录
            for d in self._build_this_cycle:
                rec = {
                    "role": d.get("role","?"), "star": d.get("star",4),
                    "constellation": d.get("constellation",0), "level": d.get("level",1),
                    "damage": d.get("avg_damage",0),
                    "weapon": d.get("weapon",{}).get("name","?") if isinstance(d.get("weapon"),dict) else str(d.get("weapon","?")),
                    "arti": d.get("artifact_set",{}).get("name", d.get("artifact_set",{}).get("set_name","?")) if isinstance(d.get("artifact_set"),dict) else str(d.get("artifact_set","?")),
                }
                self.r.lpush("build:recent", json.dumps(rec, ensure_ascii=False))
            self.r.ltrim("build:recent", 0, 19)  # 只保留最近20条

            # ---- HotChar → Redis (每 3 秒) ----
            if now - last_hotchar_flush >= HOTCHAR_FLUSH:
                last_hotchar_flush = now
                self.flush_hotchar_redis()

            # ---- HotChar → MySQL (每 2 小时) ----
            if now - last_mysql_flush >= MYSQL_INTERVAL:
                last_mysql_flush = now
                self.flush_hotchar_mysql()

            i += 1
            if i % 30 == 0:
                print(f"[Sim] 运行中... gacha窗口={len(gw)} build窗口={len(self.build_window)} hotchar窗口={len(self.hotchar_window)}", end="\r")
            time.sleep(1)

    def flush_hotchar_redis(self):
        """平替 hot_char_aggregator.py 的 Redis 刷新"""
        from collections import defaultdict
        char_stats = defaultdict(lambda: {"count":0,"star":5,"constellation_sum":0,"damage_sum":0,
            "weapons":defaultdict(int),"artifacts":defaultdict(int),"weapon_icons":{},"arti_icons":{},"avatar":"","ename":""})
        for _, d in self.hotchar_window:
            role = d["role"]
            st = char_stats[role]
            st["count"] += 1; st["star"] = d["star"]
            st["constellation_sum"] += d.get("constellation", 0)
            st["damage_sum"] += d.get("avg_damage", 0)
            w = d.get("weapon", {})
            wname = w.get("name", "?") if isinstance(w, dict) else str(w)
            st["weapons"][wname] += 1
            if isinstance(w, dict) and w.get("avatar"): st["weapon_icons"][wname] = w["avatar"]
            a = d.get("artifact_set", {})
            aname = a.get("name", "?") if isinstance(a, dict) else str(a)
            st["artifacts"][aname] += 1
            if isinstance(a, dict) and a.get("avatars"): st["arti_icons"][aname] = a["avatars"][0]
            if not st["avatar"]: st["avatar"] = d.get("avatar", "")
            if not st["ename"]: st["ename"] = d.get("ename", "")

        TOP_N = 4
        sorted_chars = sorted(char_stats.items(), key=lambda x: x[1]["count"], reverse=True)[:TOP_N]
        result = []
        for role_name, st in sorted_chars:
            cnt = st["count"]
            tw = sum(st["weapons"].values())
            wlist = [{"name":n,"count":c,"ratio":round(c/tw,2) if tw>0 else 0,"icon":st["weapon_icons"].get(n,"")}
                     for n,c in sorted(st["weapons"].items(), key=lambda x: -x[1])]
            ta = sum(st["artifacts"].values())
            alist = [{"name":n,"count":c,"ratio":round(c/ta,2) if ta>0 else 0,"icon":st["arti_icons"].get(n,"")}
                     for n,c in sorted(st["artifacts"].items(), key=lambda x: -x[1])]
            result.append({"role":role_name,"star":st["star"],"count":cnt,
                "avg_constellation":round(st["constellation_sum"]/cnt,1) if cnt>0 else 0,
                "avg_damage":int(st["damage_sum"]/cnt) if cnt>0 else 0,
                "weapons":wlist,"artifacts":alist,"avatar":st["avatar"],"ename":st["ename"]})
        self.r.set("build:hot_chars", json.dumps(result, ensure_ascii=False))
        names = ", ".join(f"{c['role']}({c['count']})" for c in result)
        print(f"[HotChar] Redis TOP{len(result)}: {names}")

    def flush_hotchar_mysql(self):
        """平替 hot_char_aggregator.py 的 MySQL 落库"""
        from collections import defaultdict
        char_stats = defaultdict(lambda: {"count":0,"star":5,"constellation_sum":0,"damage_sum":0,
            "weapons":defaultdict(int),"artifacts":defaultdict(int),"weapon_icons":{},"arti_icons":{},"avatar":"","ename":""})
        for _, d in self.hotchar_window:
            role = d["role"]
            st = char_stats[role]
            st["count"] += 1; st["star"] = d["star"]
            st["constellation_sum"] += d.get("constellation", 0)
            st["damage_sum"] += d.get("avg_damage", 0)
            w = d.get("weapon", {})
            wname = w.get("name", "?") if isinstance(w, dict) else str(w)
            st["weapons"][wname] += 1
            if isinstance(w, dict) and w.get("avatar"): st["weapon_icons"][wname] = w["avatar"]
            a = d.get("artifact_set", {})
            aname = a.get("name", "?") if isinstance(a, dict) else str(a)
            st["artifacts"][aname] += 1
            if isinstance(a, dict) and a.get("avatars"): st["arti_icons"][aname] = a["avatars"][0]
            if not st["avatar"]: st["avatar"] = d.get("avatar", "")
            if not st["ename"]: st["ename"] = d.get("ename", "")

        c = self.conn.cursor()
        c.execute("DELETE FROM rt_build_hot_char_detail WHERE snapshot_time < NOW() - INTERVAL %s SECOND", (MYSQL_RETENTION,))
        snapshot_time = time.strftime("%Y-%m-%d %H:%M:%S")
        sql = """INSERT INTO rt_build_hot_char_detail
                 (snapshot_time, role_name, star, count, avg_constellation, avg_damage,
                  top_weapon, top_weapon_icon, top_weapon_ratio,
                  top_artifact, top_artifact_icon, top_artifact_ratio, avatar)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        total = 0
        for role_name, st in char_stats.items():
            cnt = st["count"]
            if cnt == 0: continue
            tw = sum(st["weapons"].values())
            top_w = sorted(st["weapons"].items(), key=lambda x: -x[1])[0] if st["weapons"] else ("?", 0)
            ta = sum(st["artifacts"].values())
            top_a = sorted(st["artifacts"].items(), key=lambda x: -x[1])[0] if st["artifacts"] else ("?", 0)
            c.execute(sql, (snapshot_time, role_name, st["star"], cnt,
                round(st["constellation_sum"]/cnt, 1) if cnt>0 else 0,
                int(st["damage_sum"]/cnt) if cnt>0 else 0,
                top_w[0], st["weapon_icons"].get(top_w[0], ""),
                round(top_w[1]/tw, 2) if tw>0 else 0,
                top_a[0], st["arti_icons"].get(top_a[0], ""),
                round(top_a[1]/ta, 2) if ta>0 else 0,
                st["avatar"]))
            total += 1
        self.conn.commit()
        c.close()
        print(f"[HotChar] MySQL: {total}角色, 快照={snapshot_time}")


def main():
    build_data = load_build_source()
    gacha_data = load_gacha_source()
    if not build_data:
        print("[FATAL] 角色练度统计.json 不存在"); sys.exit(1)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5)
    try: r.ping()
    except Exception as e:
        print(f"[FATAL] Redis 连不上 {REDIS_HOST}:{REDIS_PORT}: {e}"); sys.exit(1)

    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB, charset="utf8mb4")

    print("=" * 60)
    print("逐风数据洞察平台 — 管道仿真器（持续运行）")
    print(f"Build: {BUILD_PER_SEC}/s | Gacha: {GACHA_PER_SEC}/s")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT} | MySQL: {MYSQL_HOST}/{MYSQL_DB}")
    print("大屏刷新页面即可看到实时数据滚动")
    print("=" * 60)

    sim = PipelineSimulator(build_data, gacha_data, r, conn)
    try:
        sim.run_forever()
    except KeyboardInterrupt:
        print("\n[Sim] 停止")
        sim.running = False
    finally:
        r.close()
        conn.close()

if __name__ == "__main__":
    main()
