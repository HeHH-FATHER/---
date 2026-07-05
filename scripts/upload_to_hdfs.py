#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐风数据洞察平台 — 数据清洗 & HDFS上传脚本
功能：读取提瓦特数据/中的JSON文件，展平嵌套结构，输出CSV并上传到HDFS

输出文件：
  /data/abyss/abyss_usage.csv    — 深渊使用率（60期 × 115角色）
  /data/rank2/rank2_usage.csv    — 幽境危战使用率（8期 × 114角色）
  /data/team/team_ranking.csv    — 配队排行（1,320支队伍）
  /data/role/role_avg.csv        — 角色练度（122角色）
  /data/role/role_vote.csv       — 满意度（121角色）
  /data/role/role_list.csv       — 角色字典（123角色）
"""

import json
import csv
import os
import sys
import glob
import subprocess

# ===================== 配置 =====================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "提瓦特数据")
RAW_DIR = os.path.join(DATA_DIR, "原始数据")
OUTPUT_DIR = os.path.join(BASE_DIR, "output_csv")

# HDFS 目标路径
HDFS_TARGETS = {
    "abyss_usage.csv": "/data/abyss/",
    "rank2_usage.csv": "/data/rank2/",
    "team_ranking.csv": "/data/team/",
    "role_avg.csv": "/data/role/",
    "role_vote.csv": "/data/role/",
    "role_list.csv": "/data/role/",
}

# 版本号映射：深渊版本历史 value → version_name
VERSION_MAP = {}  # {value: title}  e.g. {59: "v6.6(第一期)"}


def load_version_map():
    """加载深渊版本历史，建立 value → title 映射"""
    version_file = os.path.join(DATA_DIR, "深渊版本历史.json")
    if os.path.exists(version_file):
        with open(version_file, "r", encoding="utf-8") as f:
            versions = json.load(f)
        for v in versions:
            VERSION_MAP[v["value"]] = v["title"]
        print(f"[INFO] 加载版本映射: {len(VERSION_MAP)} 个版本")
    else:
        print("[WARN] 未找到深渊版本历史.json，将使用原始title作为版本名")


def get_abyss_version_from_file(filepath):
    """从原始abyss JSON文件中提取版本名称"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    title = data.get("title", "")
    # "6.6深渊使用率统计(第一期)" → "v6.6(第一期)"
    if "深渊使用率统计" in title:
        title = "v" + title.replace("深渊使用率统计", "")
    return title


def get_rank2_version_from_file(filepath):
    """从原始rank2 JSON文件中提取版本名称"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    title = data.get("title", "")
    # "6.6幽境危战使用率统计" → "v6.6"
    if "幽境危战使用率统计" in title:
        title = "v" + title.replace("幽境危战使用率统计", "")
    return title


# ===================== 任务1：深渊使用率 =====================
def process_abyss_usage():
    """
    处理原始数据/abyss_v*.json → abyss_usage.csv
    每个文件是一个版本，包含按tier分组的角色列表
    字段: version, name, star, use, own, use_rate, own_rate, tier
    """
    print("\n[1/6] 处理深渊使用率数据...")
    files = sorted(glob.glob(os.path.join(RAW_DIR, "abyss_v*.json")))
    if not files:
        print("[WARN] 未找到 abyss_v*.json，尝试使用聚合文件...")
        return process_simple_json("深渊角色使用率.json", "abyss_usage.csv",
                                   ["version", "name", "star", "use", "own", "use_rate", "own_rate", "tier"],
                                   version_default="latest")

    all_rows = []
    for fpath in files:
        version_name = get_abyss_version_from_file(fpath)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        result = data.get("result", [])
        # result[0] = 按tier分组; result[1] = 扁平角色列表(含rank_class)
        char_list = []
        if len(result) >= 2 and isinstance(result[1], list):
            char_list = result[1]
        elif isinstance(result, list) and result and isinstance(result[0], dict):
            # 兼容直接是角色对象数组的情况
            char_list = result

        for char in char_list:
            all_rows.append({
                "version": version_name,
                "name": char.get("name", ""),
                "star": char.get("star", 0),
                "use": char.get("use", 0),
                "own": char.get("own", 0),
                "use_rate": char.get("use_rate", 0),
                "own_rate": char.get("own_rate", 0),
                "tier": char.get("rank_class", ""),
            })

    output_path = os.path.join(OUTPUT_DIR, "abyss_usage.csv")
    write_csv(output_path,
              ["version", "name", "star", "use", "own", "use_rate", "own_rate", "tier"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录, {len(files)} 个版本)")
    return output_path


# ===================== 任务2：幽境危战使用率 =====================
def process_rank2_usage():
    """
    处理原始数据/rank2_v*.json → rank2_usage.csv
    字段: version, name, star, use, own, use_rate, own_rate, tier
    """
    print("\n[2/6] 处理幽境危战使用率数据...")
    files = sorted(glob.glob(os.path.join(RAW_DIR, "rank2_v*.json")))
    if not files:
        print("[WARN] 未找到 rank2_v*.json，尝试使用聚合文件...")
        return process_simple_json("幽境危战使用率.json", "rank2_usage.csv",
                                   ["version", "name", "star", "use", "own", "use_rate", "own_rate", "tier"],
                                   version_default="latest")

    all_rows = []
    for fpath in files:
        version_name = get_rank2_version_from_file(fpath)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        result = data.get("result", [])
        # result[0] = 按tier分组; result[1] = 扁平角色列表(含rank_class)
        char_list = []
        if len(result) >= 2 and isinstance(result[1], list):
            char_list = result[1]
        elif isinstance(result, list) and result and isinstance(result[0], dict):
            char_list = result

        for char in char_list:
            all_rows.append({
                "version": version_name,
                "name": char.get("name", ""),
                "star": char.get("star", 0),
                "use": char.get("use", 0),
                "own": char.get("own", 0),
                "use_rate": char.get("use_rate", 0),
                "own_rate": char.get("own_rate", 0),
                "tier": char.get("rank_class", ""),
            })

    output_path = os.path.join(OUTPUT_DIR, "rank2_usage.csv")
    write_csv(output_path,
              ["version", "name", "star", "use", "own", "use_rate", "own_rate", "tier"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录, {len(files)} 个版本)")
    return output_path


# ===================== 任务3：配队排行 =====================
def process_team_ranking():
    """
    处理 深渊配队汇总.json → team_ranking.csv
    字段: version, team, use, use_rate, attend_rate, has_rate, up_use, down_use
    """
    print("\n[3/6] 处理配队排行数据...")
    src = os.path.join(DATA_DIR, "深渊配队汇总.json")
    if not os.path.exists(src):
        print("[ERROR] 未找到 深渊配队汇总.json")
        return None

    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []
    for item in data:
        all_rows.append({
            "version": item.get("version", ""),
            "team": item.get("team", ""),
            "use": item.get("use", 0),
            "use_rate": item.get("use_rate", 0),
            "attend_rate": item.get("attend_rate", 0),
            "has_rate": item.get("has_rate", 0),
            "up_use": item.get("up_use", 0),
            "down_use": item.get("down_use", 0),
        })

    output_path = os.path.join(OUTPUT_DIR, "team_ranking.csv")
    write_csv(output_path,
              ["version", "team", "use", "use_rate", "attend_rate", "has_rate", "up_use", "down_use"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录)")
    return output_path


# ===================== 任务4：角色练度 =====================
def process_role_avg():
    """
    处理 角色练度统计.json → role_avg.csv
    字段: name, star, player_count, avg_level, avg_const, avg_damage, damage_type, top_weapon, top_artifact
    """
    print("\n[4/6] 处理角色练度数据...")
    src = os.path.join(DATA_DIR, "角色练度统计.json")
    if not os.path.exists(src):
        print("[ERROR] 未找到 角色练度统计.json")
        return None

    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []
    for item in data:
        # 提取使用率最高的武器
        top_weapon = ""
        top_weapon_rate = 0
        weapons = item.get("weapons", [])
        for w in weapons:
            if w.get("rate", 0) > top_weapon_rate:
                top_weapon_rate = w["rate"]
                top_weapon = w.get("name", "")

        # 提取使用率最高的圣遗物
        top_artifact = ""
        top_artifact_rate = 0
        artifacts = item.get("artifact_sets", [])
        for a in artifacts:
            if a.get("rate", 0) > top_artifact_rate:
                top_artifact_rate = a["rate"]
                top_artifact = a.get("name", "")

        all_rows.append({
            "name": item.get("role", ""),
            "star": item.get("star", 0),
            "player_count": item.get("player_count", 0),
            "avg_level": item.get("avg_level", 0),
            "avg_const": item.get("avg_constellation", 0),
            "avg_damage": item.get("avg_damage", 0),
            "damage_type": item.get("damage_type", ""),
            "top_weapon": top_weapon,
            "top_artifact": top_artifact,
        })

    output_path = os.path.join(OUTPUT_DIR, "role_avg.csv")
    write_csv(output_path,
              ["name", "star", "player_count", "avg_level", "avg_const",
               "avg_damage", "damage_type", "top_weapon", "top_artifact"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录)")
    return output_path


# ===================== 任务5：角色满意度 =====================
def process_role_vote():
    """
    处理 角色满意度排行.json → role_vote.csv
    字段: name, star, avg_ability, avg_look, avg_satify, vote_sum, favorite
    """
    print("\n[5/6] 处理角色满意度数据...")
    src = os.path.join(DATA_DIR, "角色满意度排行.json")
    if not os.path.exists(src):
        print("[ERROR] 未找到 角色满意度排行.json")
        return None

    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []
    for item in data:
        all_rows.append({
            "name": item.get("role", ""),
            "star": item.get("star", 0),
            "avg_ability": item.get("avg_ability", 0),
            "avg_look": item.get("avg_look", 0),
            "avg_satify": item.get("avg_satify", 0),
            "vote_sum": item.get("vote_sum", 0),
            "favorite": item.get("favorite", 0),
        })

    output_path = os.path.join(OUTPUT_DIR, "role_vote.csv")
    write_csv(output_path,
              ["name", "star", "avg_ability", "avg_look", "avg_satify", "vote_sum", "favorite"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录)")
    return output_path


# ===================== 任务6：角色列表 =====================
def process_role_list():
    """
    处理 角色列表_clean.json → role_list.csv
    字段: name, star, avatar
    """
    print("\n[6/6] 处理角色列表数据...")
    src = os.path.join(DATA_DIR, "角色列表_clean.json")
    if not os.path.exists(src):
        print("[ERROR] 未找到 角色列表_clean.json")
        return None

    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []
    for item in data:
        all_rows.append({
            "name": item.get("name", ""),
            "star": item.get("star", 0),
            "avatar": item.get("avatar", ""),
        })

    output_path = os.path.join(OUTPUT_DIR, "role_list.csv")
    write_csv(output_path,
              ["name", "star", "avatar"],
              all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录)")
    return output_path


# ===================== 辅助函数 =====================
def process_simple_json(filename, output_name, fieldnames, version_default=""):
    """通用处理：简单JSON数组 → CSV（无版本信息时使用）"""
    src = os.path.join(DATA_DIR, filename)
    if not os.path.exists(src):
        print(f"[ERROR] 未找到 {filename}")
        return None

    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []
    field_map = {
        # abyss/rank2 fields
        "use": "use_count", "own": "own_count",
        "use_count": "use", "own_count": "own",
    }

    for item in data:
        row = {}
        for fname in fieldnames:
            if fname == "version":
                row[fname] = version_default
            elif fname == "tier":
                row[fname] = item.get("tier", item.get("rank", ""))
            elif fname == "use":
                row[fname] = item.get("use", item.get("use_count", 0))
            elif fname == "own":
                row[fname] = item.get("own", item.get("own_count", 0))
            else:
                row[fname] = item.get(fname, "")
        all_rows.append(row)

    output_path = os.path.join(OUTPUT_DIR, output_name)
    write_csv(output_path, fieldnames, all_rows)
    print(f"  → 输出: {output_path} ({len(all_rows)} 条记录)")
    return output_path


def write_csv(filepath, fieldnames, rows):
    """写入CSV文件（UTF-8 with BOM, 逗号分隔）"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upload_to_hdfs(local_path, hdfs_dir):
    """上传CSV文件到HDFS"""
    filename = os.path.basename(local_path)
    hdfs_path = hdfs_dir.rstrip("/") + "/" + filename
    try:
        # 创建HDFS目录
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
                       check=False, capture_output=True)
        # 上传文件（覆盖）
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
            check=False, capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  [HDFS] ✓ {filename} → {hdfs_path}")
            return True
        else:
            print(f"  [HDFS] ✗ {filename} 上传失败: {result.stderr}")
            return False
    except FileNotFoundError:
        print(f"  [HDFS] ⚠ hdfs 命令不可用，跳过上传（文件已保存到本地: {local_path}）")
        return False


# ===================== 主流程 =====================
def main():
    print("=" * 60)
    print("逐风数据洞察平台 — 数据清洗 & HDFS上传")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    load_version_map()

    # 是否需要上传到HDFS
    do_upload = "--upload" in sys.argv or "-u" in sys.argv

    tasks = [
        ("abyss_usage.csv", process_abyss_usage, "/data/abyss/"),
        ("rank2_usage.csv", process_rank2_usage, "/data/rank2/"),
        ("team_ranking.csv", process_team_ranking, "/data/team/"),
        ("role_avg.csv", process_role_avg, "/data/role/"),
        ("role_vote.csv", process_role_vote, "/data/role/"),
        ("role_list.csv", process_role_list, "/data/role/"),
    ]

    results = {}
    for name, task_fn, hdfs_dir in tasks:
        try:
            output_path = task_fn()
            if output_path:
                results[name] = output_path
                if do_upload:
                    upload_to_hdfs(output_path, hdfs_dir)
        except Exception as e:
            print(f"  [ERROR] {name} 处理失败: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"处理完成！共生成 {len(results)} 个CSV文件")
    print(f"输出目录: {OUTPUT_DIR}")
    if not do_upload:
        print("提示: 使用 --upload 参数可自动上传到HDFS")
    print("=" * 60)
    return results


if __name__ == "__main__":
    main()
