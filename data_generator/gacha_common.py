#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 抽卡数据公共模块
gacha_producer.py（CSV/HDFS） 和 gacha_kafka_producer.py（Kafka）共享逻辑

数据源: 提瓦特数据/卡池统计_clean.json
"""
import json
import os
import random
from datetime import datetime

# ==================== 配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "提瓦特数据")

BASE_RATE_5STAR = 0.006
PITY_SOFT = 74
PITY_HARD = 90
BASE_UID = 100000000
BASE_TS = int(datetime(2022, 8, 24).timestamp())


def load_banners(data_dir=None):
    """加载卡池统计数据"""
    src = os.path.join(data_dir or DATA_DIR, "卡池统计_clean.json")
    if not os.path.exists(src):
        raise FileNotFoundError(f"卡池数据不存在: {src}")
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    banners = []
    for item in data.get("roleList", []):
        if item.get("type") != "role":
            continue
        content = item.get("content", {})
        if not content:
            continue
        total_pulls = sum(content.values())
        banners.append({
            "version": item.get("version", ""),
            "start": item.get("start_time", ""),
            "end": item.get("end_time", ""),
            "chars": content,
            "total": total_pulls,
            "weight": 0
        })

    grand_total = sum(b["total"] for b in banners)
    for b in banners:
        b["weight"] = b["total"] / grand_total if grand_total > 0 else 0

    return banners


def pick_char(banner):
    """从卡池中按权重随机选一个角色"""
    chars = list(banner["chars"].keys())
    weights = list(banner["chars"].values())
    return random.choices(chars, weights=weights, k=1)[0]


def get_rate(pity_count):
    """软保底机制：74抽后概率线性增加"""
    if pity_count >= PITY_SOFT:
        return BASE_RATE_5STAR + (pity_count - PITY_SOFT + 1) * 0.06
    return BASE_RATE_5STAR


def generate_record(banners, pity_state):
    """
    生成一条抽卡记录
    返回: (record_dict, updated_pity_state)
    """
    banner = random.choices(banners, weights=[b["weight"] for b in banners], k=1)[0]
    uid = BASE_UID + random.randint(0, 99999999)

    try:
        t_start = int(datetime.strptime(banner["start"], "%Y-%m-%d").timestamp())
        t_end = int(datetime.strptime(banner["end"], "%Y-%m-%d").timestamp())
    except Exception:
        t_start = BASE_TS
        t_end = BASE_TS + 86400 * 21

    ts = random.randint(t_start, t_end)
    char = pick_char(banner)

    pity = pity_state.get(uid, 0) + 1
    current_rate = get_rate(pity)
    is_5star = random.random() < current_rate

    if pity >= PITY_HARD:
        is_5star = True
        pity = 0

    star = 5 if is_5star else 4
    if is_5star:
        pity_state[uid] = 0
    else:
        pity_state[uid] = pity

    record = {
        "uid": str(uid),
        "banner": banner["version"],
        "item": char,
        "star": star,
        "timestamp": ts,
        "pity_count": pity
    }
    return record, pity_state
