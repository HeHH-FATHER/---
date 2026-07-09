#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — ODS 层 抽卡数据生成
═══════════════════════════════════════════════════════════════
功能: 模拟原神真实保底机制的逐抽记录生成器。
      每抽记录含 star(3/4/5)、item、type、保底状态。

层级: ODS — 原始逐抽数据
下游: DWD 层 load_gacha_ads.py → 过滤五星 → 聚合 → MySQL

用法:
  python3 preprocess_gacha.py                    # 单次 5000 抽
  python3 preprocess_gacha.py --count 10000     # 指定抽数
  python3 preprocess_gacha.py --loop 300        # 每5分钟循环
═══════════════════════════════════════════════════════════════
"""
import json, time, random, sys, os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "提瓦特数据")


# ═══════════════════════════════════════════════
# 保底抽卡模拟器
# ═══════════════════════════════════════════════

class GachaPullSimulator:
    """逐抽模拟，含原神保底机制。每个玩家独立跟踪保底计数和 50/50 状态。"""

    def __init__(self, seed=None, player_count=5000):
        self.rng = random.Random(seed)
        self.player_count = player_count
        # 每个玩家独立状态
        self.pity_5 = {uid: 0 for uid in range(player_count)}
        self.pity_4 = {uid: 0 for uid in range(player_count)}
        self.guaranteed = {uid: False for uid in range(player_count)}

    def single_pull(self, banner_up, uid=None):
        """
        某玩家单抽一次。uid=None 时随机分配。
        banner_up: [(name, type), ...] 当前卡池UP列表。
        返回 {"star": 3|4|5, "item": "...", "type": "role"|"weapon", "is_up": bool, "uid": int}
        """
        if uid is None:
            uid = self.rng.randint(0, self.player_count - 1)

        self.pity_4[uid] += 1
        self.pity_5[uid] += 1
        p5 = self.pity_5[uid]
        guaranteed = self.guaranteed[uid]

        # ── 五星判定 ──
        # 原神综合五星率 1.6%（含保底）。软保底从 74 开始，每抽 +6% 直到 90 抽硬保底
        if p5 <= 73:
            five_rate = 0.006
        elif p5 <= 89:
            five_rate = 0.006 + (p5 - 73) * 0.075  # 74抽起每抽+7.5%，确保综合约1.6%
        else:
            five_rate = 1.0  # 90 抽硬保底

        if self.rng.random() < five_rate:
            self.pity_5[uid] = 0
            if guaranteed or self.rng.random() < 0.5:
                self.guaranteed[uid] = False
                up = random.choice(banner_up) if banner_up else ("?", "role")
                return {"star": 5, "item": up[0], "type": up[1], "is_up": True, "uid": uid}
            else:
                self.guaranteed[uid] = True
                r = self._std_5star()
                r["uid"] = uid
                return r

        # ── 四星判定 ──
        if self.pity_4[uid] >= 10 or self.rng.random() < 0.051:
            self.pity_4[uid] = 0
            r = self._random_4star()
            r["uid"] = uid
            return r

        # ── 三星 ──
        r = self._random_3star()
        r["uid"] = uid
        return r

    def _std_5star(self):
        names = ["迪卢克","琴","莫娜","七七","刻晴","提纳里","迪希雅","梦见月瑞希"]
        return {"star": 5, "item": self.rng.choice(names), "type": "role", "is_up": False}

    def _random_4star(self):
        items = ["班尼特","行秋","香菱","菲谢尔","砂糖","迪奥娜","北斗","凝光",
                 "重云","诺艾尔","芭芭拉","凯亚","丽莎","安柏",
                 "西风剑","祭礼剑","笛剑","西风大剑","西风长枪","匣里灭辰",
                 "西风猎弓","祭礼弓","绝弦","西风秘典","祭礼残章","流浪乐章"]
        return {"star": 4, "item": self.rng.choice(items), "type": "weapon" if self.rng.random() < 0.5 else "role"}

    def _random_3star(self):
        items = ["弹弓","飞天御剑","黑缨枪","魔导绪论","讨龙英杰谭"]
        return {"star": 3, "item": self.rng.choice(items), "type": "weapon"}


# ═══════════════════════════════════════════════
# 加载卡池 UP 列表
# ═══════════════════════════════════════════════

def load_banner_items(version="6.6下半"):
    """加载指定版本卡池 UP 列表，默认 6.6下半"""
    path = os.path.join(DATA_DIR, "卡池统计_clean.json")
    items = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_entries = []
        for lst_name, ptype in [("roleList", "role"), ("weaponList", "weapon")]:
            for entry in data.get(lst_name, []):
                all_entries.append((entry.get("version", ""), entry, ptype))
        for ver, entry, ptype in all_entries:
            if ver == version:
                for name in entry.get("content", {}).keys():
                    items.append((name, ptype))
    except: pass
    if not items: items = [("玛薇卡", "role"), ("洛恩", "role"), ("焚曜千阳", "weapon"), ("灾悔", "weapon")]
    return items


# ═══════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════

def generate_pulls(count=5000, player_count=80):
    """返回 count 条逐抽记录。player_count 控制独立玩家数（越少每人抽越多，越容易触发保底）"""
    banner = load_banner_items()
    sim = GachaPullSimulator(player_count=player_count)
    return [sim.single_pull(banner) for _ in range(count)]


def main():
    count = 5000
    player_count = 80
    loop_interval = None
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--count" and i+1 < len(args): count = int(args[i+1])
        elif a == "--players" and i+1 < len(args): player_count = int(args[i+1])
        elif a == "--loop" and i+1 < len(args): loop_interval = int(args[i+1])


    print("=" * 60, file=sys.stderr)
    print("逐风数据洞察平台 — ODS 层 抽卡数据生成", file=sys.stderr)
    print(f"卡池 UP 物品: {len(load_banner_items())} 个", file=sys.stderr)
    print(f"模式: {'每' + str(loop_interval) + 's 循环' if loop_interval else '单次'}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    while True:
        records = generate_pulls(count, player_count)
        five = sum(1 for r in records if r["star"] == 5)
        four = sum(1 for r in records if r["star"] == 4)
        print(f"[ODS] 生成 {len(records)} 抽 → 5★={five} 4★={four} 3★={len(records)-five-four}", file=sys.stderr)
        # 输出 JSON 供下游管道消费
        for r in records:
            print(json.dumps(r, ensure_ascii=False))
        if not loop_interval: break
        time.sleep(loop_interval)


if __name__ == "__main__":
    main()
