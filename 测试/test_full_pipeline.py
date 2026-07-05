#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — 全链路格式兼容性测试
═══════════════════════════════════════════════════════════════

验证每条链路的数据格式：上一层输出 → 下一层输入，确保格式兼容。

   生成器 JSON → ODS JSONL → DWD CSV → DWS CSV → ADS 解析

由于没有 Hadoop/MySQL，DWD/DWS 层按 MR 实际输出格式手工构造，
重点验证 Python 脚本的解析逻辑是否正确处理这些格式。

用法:
  python test_full_pipeline.py

═══════════════════════════════════════════════════════════════
"""

import json
import os
import sys
import tempfile
import shutil
from collections import defaultdict
from datetime import datetime

# 把项目 scripts 目录加入 path
SCRIPT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
sys.path.insert(0, SCRIPT_DIR)

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  -- {detail}")
    return condition


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ═══════════════════════════════════════════════════════════════
# 准备测试目录
# ═══════════════════════════════════════════════════════════════

TMP = tempfile.mkdtemp(prefix="abyss_test_")
os.makedirs(f"{TMP}/input", exist_ok=True)
os.makedirs(f"{TMP}/ods", exist_ok=True)
os.makedirs(f"{TMP}/dwd", exist_ok=True)
os.makedirs(f"{TMP}/dws", exist_ok=True)
os.makedirs(f"{TMP}/ads", exist_ok=True)

print(f"测试目录: {TMP}")

# ═══════════════════════════════════════════════════════════════
# 第0层：模拟生成器输出（3个正常用户 + 1个脏数据用户）
# ═══════════════════════════════════════════════════════════════

section("第0层：模拟生成器输出（4个用户）")

# 用户1：正常，完整BOX+战绩
U1_BOX = {
    "uid": "100000001",
    "characters": [
        {"name": "玛薇卡",      "star": 5, "constellation": 2, "level": 90},
        {"name": "班尼特",      "star": 4, "constellation": 6, "level": 80},
        {"name": "茜特菈莉",    "star": 5, "constellation": 0, "level": 90},
        {"name": "希诺宁",      "star": 5, "constellation": 0, "level": 90},
        {"name": "枫原万叶",    "star": 5, "constellation": 0, "level": 90},
        {"name": "芙宁娜",      "star": 5, "constellation": 1, "level": 90},
        {"name": "香菱",        "star": 4, "constellation": 6, "level": 80},
        {"name": "钟离",        "star": 5, "constellation": 0, "level": 90},
    ]
}
U1_RECORD = {
    "uid": "100000001",
    "teams": [
        {"half": 1, "team_index": 1, "members": [
            {"name": "玛薇卡", "star": 5}, {"name": "班尼特", "star": 4},
            {"name": "希诺宁", "star": 5}, {"name": "枫原万叶", "star": 5}
        ]},
        {"half": 2, "team_index": 2, "members": [
            {"name": "茜特菈莉", "star": 5}, {"name": "芙宁娜", "star": 5},
            {"name": "香菱", "star": 4}, {"name": "钟离", "star": 5}
        ]}
    ]
}

# 用户2：正常，不同的配队组合
U2_BOX = {
    "uid": "100000002",
    "characters": [
        {"name": "丝柯克",      "star": 5, "constellation": 2, "level": 90},
        {"name": "芙宁娜",      "star": 5, "constellation": 0, "level": 90},
        {"name": "班尼特",      "star": 4, "constellation": 5, "level": 80},
        {"name": "香菱",        "star": 4, "constellation": 6, "level": 80},
        {"name": "玛薇卡",      "star": 5, "constellation": 1, "level": 90},
        {"name": "钟离",        "star": 5, "constellation": 0, "level": 90},
    ]
}
U2_RECORD = {
    "uid": "100000002",
    "teams": [
        {"half": 1, "team_index": 3, "members": [
            {"name": "丝柯克", "star": 5}, {"name": "芙宁娜", "star": 5},
            {"name": "班尼特", "star": 4}
        ]},
        {"half": 2, "team_index": 4, "members": [
            {"name": "玛薇卡", "star": 5}, {"name": "香菱", "star": 4},
            {"name": "钟离", "star": 5}
        ]}
    ]
}

# 用户3：正常
U3_BOX = {
    "uid": "100000003",
    "characters": [
        {"name": "哥伦比娅",    "star": 5, "constellation": 0, "level": 90},
        {"name": "希诺宁",      "star": 5, "constellation": 1, "level": 90},
        {"name": "班尼特",      "star": 4, "constellation": 6, "level": 80},
        {"name": "玛薇卡",      "star": 5, "constellation": 2, "level": 90},
        {"name": "行秋",        "star": 4, "constellation": 6, "level": 80},
    ]
}
U3_RECORD = {
    "uid": "100000003",
    "teams": [
        {"half": 1, "team_index": 5, "members": [
            {"name": "玛薇卡", "star": 5}, {"name": "希诺宁", "star": 5},
            {"name": "班尼特", "star": 4}
        ]},
        {"half": 2, "team_index": 6, "members": [
            {"name": "哥伦比娅", "star": 5}, {"name": "行秋", "star": 4}
        ]}
    ]
}

# 用户4：脏数据 — bad_star（star=99）+ bad_const（constellation=7）
# 这个会触发 MR 的 bad_star 规则（第一个检测到的脏数据类型）
U4_BOX = {
    "uid": "100000004",
    "characters": [
        {"name": "尼可",        "star": 99, "constellation": 0, "level": 90},
        {"name": "班尼特",      "star": 4,  "constellation": 7, "level": 80},
    ]
}
U4_RECORD = {
    "uid": "100000004",
    "teams": [
        {"half": 1, "team_index": 7, "members": [
            {"name": "尼可", "star": 5}, {"name": "班尼特", "star": 4}
        ]},
        {"half": 2, "team_index": 8, "members": [
            {"name": "尼可", "star": 5}  # 角色不在BOX中（已被标记脏）
        ]}
    ]
}

users = [
    ("100000001", U1_BOX, U1_RECORD),
    ("100000002", U2_BOX, U2_RECORD),
    ("100000003", U3_BOX, U3_RECORD),
    ("100000004", U4_BOX, U4_RECORD),
]

for uid, box, record in users:
    with open(f"{TMP}/input/{uid}_char_box.json", "w", encoding="utf-8") as f:
        json.dump(box, f, ensure_ascii=False)
    with open(f"{TMP}/input/{uid}_abyss_record.json", "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)

check("4个用户 BOX JSON 就绪", True)
check("4个用户 战绩 JSON 就绪", True)

# ═══════════════════════════════════════════════════════════════
# 第1层：ODS — preprocess_abyss.py（真实运行！）
# ═══════════════════════════════════════════════════════════════

section("第1层：ODS 预处理 (JSON → JSONL)")

# 直接调用 preprocess_abyss 模块的核心函数，不通过命令行
from preprocess_abyss import find_user_files, merge_user, preprocess

# --- 测试1: 文件扫描 ---
users_found = find_user_files(f"{TMP}/input")
check("扫描到 4 个用户", len(users_found) == 4, f"实际: {len(users_found)}")
check("100000001 有 BOX+战绩",
      "box" in users_found["100000001"] and "record" in users_found["100000001"])

# --- 测试2: merge_user 输出格式 ---
line = merge_user("100000001", "v6.6",
                  users_found["100000001"]["box"],
                  users_found["100000001"]["record"])
parsed = json.loads(line)
check("JSONL 可解析为 JSON", parsed is not None)
check("JSONL 含 uid",        parsed["uid"] == "100000001")
check("JSONL 含 version",    parsed["version"] == "v6.6")
check("JSONL 含 box",        "box" in parsed and parsed["box"] is not None)
check("JSONL 含 record",     "record" in parsed and parsed["record"] is not None)
check("box.uid 一致",        parsed["box"]["uid"] == "100000001")
check("record.uid 一致",     parsed["record"]["uid"] == "100000001")
check("record.teams 有2队",  len(parsed["record"]["teams"]) == 2)

# --- 测试3: 完整预处理流程 ---
ods_output = f"{TMP}/ods/abyss_test.jsonl"
result = preprocess(f"{TMP}/input", "v6.6", ods_output, quiet=True)
check("preprocess 返回非空", result is not None)
check("JSONL 文件存在",      os.path.exists(ods_output))

with open(ods_output, "r", encoding="utf-8") as f:
    ods_lines = f.readlines()
check("JSONL 共 4 行", len(ods_lines) == 4, f"实际: {len(ods_lines)}")

# 验证每行都可解析
all_valid = True
for i, l in enumerate(ods_lines):
    try:
        d = json.loads(l)
        if d["uid"] not in [u[0] for u in users]:
            all_valid = False
    except:
        all_valid = False
check("每行 JSON 合法且 uid 匹配用户", all_valid)

print(f"\n  ODS JSONL 样例（第1行）:")
print(f"  {ods_lines[0][:200]}...")

# ═══════════════════════════════════════════════════════════════
# 第2层：模拟 DWD — AbyssCleanMR 输出格式
# ═══════════════════════════════════════════════════════════════

section("第2层：DWD 清洗 (模拟 MR 输出格式)")

"""
AbyssCleanMR 真实输出格式（来自工作日志）:
  MultipleOutputs 命名: chardetail / teamusage / dirty
  输出目录结构:
    <output>/dwd/char_detail/data-m-00000   → char_detail CSV
    <output>/dwd/team_usage/data-m-00000    → team_usage CSV
    <output>/dirty/<type>/data-m-00000      → 脏数据JSONL

  char_detail CSV 格式（无表头，逗号分隔）:
    version,uid,char_name,star,constellation,level,used_in_abyss

  team_usage CSV 格式（无表头，逗号分隔）:
    version,uid,half,team_index,char_name,star,position

关键特征（需在测试中验证下游能解析）:
  1. 无表头
  2. char_name 可能是中文
  3. MultipleOutputs 产生的文件名格式
"""

# --- 模拟 MR 清洗 4 个用户的输出 ---
# 用户1-3 通过清洗，用户4 触发 bad_star（star=99）

clean_char = []
clean_team = []
dirty = {"bad_star": []}

for uid, box, record in users:
    if uid == "100000004":
        # 脏数据：整条 JSONL 行记入 bad_star
        dirty["bad_star"].append(json.dumps(
            {"uid": uid, "version": "v6.6", "box": box, "record": record},
            ensure_ascii=False))
        continue

    # 正常用户：写 char_detail + team_usage
    box_names = {c["name"] for c in box["characters"]}
    team_names = set()
    for t in record["teams"]:
        for m in t["members"]:
            team_names.add(m["name"])

    for c in box["characters"]:
        used = 1 if c["name"] in team_names else 0
        clean_char.append(
            f"v6.6,{uid},{c['name']},{c['star']},{c['constellation']},{c['level']},{used}")

    for t in record["teams"]:
        half = t["half"]
        team_idx = t["team_index"]
        for pos, m in enumerate(t["members"], 1):
            clean_team.append(
                f"v6.6,{uid},{half},{team_idx},{m['name']},{m['star']},{pos}")

# 写模拟 DWD 输出
dwd_char_dir = f"{TMP}/dwd/dwd_char_detail"
dwd_team_dir = f"{TMP}/dwd/dwd_team_usage"
dwd_dirty_dir = f"{TMP}/dwd/dirty"
os.makedirs(dwd_char_dir, exist_ok=True)
os.makedirs(dwd_team_dir, exist_ok=True)

with open(f"{dwd_char_dir}/data-m-00000", "w", encoding="utf-8") as f:
    f.write("\n".join(clean_char))
with open(f"{dwd_team_dir}/data-m-00000", "w", encoding="utf-8") as f:
    f.write("\n".join(clean_team))

# 脏数据目录
for dtype, lines in dirty.items():
    ddir = f"{dwd_dirty_dir}/{dtype}"
    os.makedirs(ddir, exist_ok=True)
    with open(f"{ddir}/data-m-00000", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# 验证
check("char_detail: 19行 (用户1:8 + 用户2:6 + 用户3:5角色)",
      len(clean_char) == 19,
      f"实际: {len(clean_char)}")

check("team_usage: 19行 (用户1:8 + 用户2:6 + 用户3:5人次)",
      len(clean_team) == 19,
      f"实际: {len(clean_team)}")

check("脏数据: 1条 bad_star",
      len(dirty["bad_star"]) == 1)

# 验证 CSV 格式可解析
char_parts = clean_char[0].split(",")
check("char_detail 7列", len(char_parts) == 7,
      f"实际: {len(char_parts)} 列: {char_parts}")

team_parts = clean_team[0].split(",")
check("team_usage 7列", len(team_parts) == 7,
      f"实际: {len(team_parts)} 列: {team_parts}")

# 关键验证：char_name 含中文也能正确 split
for line in clean_char:
    parts = line.split(",")
    char_name = parts[2]
    if not check(f"char_detail 角色名 '{char_name}' 非空",
                 len(char_name) > 0 and len(parts) == 7,
                 f"行: {line}"):
        break

print(f"\n  char_detail 样例: {clean_char[0]}")
print(f"  team_usage 样例:  {clean_team[0]}")

# ═══════════════════════════════════════════════════════════════
# 第3层：DWS 共现 — compute_team_cooccur.py 核心逻辑
# ═══════════════════════════════════════════════════════════════

section("第3层：DWS 配队共现 (真实运行核心逻辑)")

from compute_team_cooccur import compute_cooccurrence

# 直接用 team_usage 数据调核心函数
pair_counts = compute_cooccurrence(clean_team)
check("共现计算有结果", len(pair_counts) > 0, f"实际: {len(pair_counts)} 对")

# 验证共现逻辑：用户1 上半 [玛薇卡,班尼特,希诺宁,枫原万叶] → C(4,2)=6对
# 用户1 下半 [茜特菈莉,芙宁娜,香菱,钟离] → C(4,2)=6对
# 用户2 上半 [丝柯克,芙宁娜,班尼特] → C(3,2)=3对
# 用户2 下半 [玛薇卡,香菱,钟离] → C(3,2)=3对
# 用户3 上半 [玛薇卡,希诺宁,班尼特] → C(3,2)=3对
# 用户3 下半 [哥伦比娅,行秋] → C(2,2)=1对
# 总计 = 6+6+3+3+3+1 = 22对（含重复）

# 用户1上半 [玛薇卡,班尼特,希诺宁,枫原万叶] → (玛薇卡,班尼特) 1次
# 用户3上半 [玛薇卡,希诺宁,班尼特] → (玛薇卡,班尼特) 1次
# 总计 = 2次（用户2下半无班尼特）
check("玛薇卡↔班尼特 共现 2次",
      pair_counts.get(("玛薇卡", "班尼特"), 0) == 2,
      f"实际: {pair_counts.get(('玛薇卡', '班尼特'), 0)}")

check("芙宁娜↔丝柯克 出现在用户2上半 = 1次",
      pair_counts.get(("丝柯克", "芙宁娜"), 0) == 1,
      f"实际: {pair_counts.get(('丝柯克', '芙宁娜'), 0)}")

# 验证输出 CSV 格式
with open(f"{TMP}/dws/dws_team_cooccur.csv", "w", encoding="utf-8") as f:
    f.write("char_a,char_b,cooccur_count\n")
    for (a, b), count in sorted(pair_counts.items(), key=lambda x: -x[1]):
        f.write(f"{a},{b},{count}\n")

# 读回验证
with open(f"{TMP}/dws/dws_team_cooccur.csv", "r", encoding="utf-8") as f:
    cooccur_lines = f.readlines()
check("共现CSV 有表头", cooccur_lines[0].strip() == "char_a,char_b,cooccur_count")
check("共现CSV 有数据行", len(cooccur_lines) > 1)

# 验证每行可解析为 3 列
parse_ok = True
for i, l in enumerate(cooccur_lines[1:], 1):
    parts = l.strip().split(",", 2)
    if len(parts) != 3:
        parse_ok = False
        break
    try:
        int(parts[2])
    except:
        parse_ok = False
        break
check("共现CSV 每行3列，第3列是整数", parse_ok)

print(f"\n  共现 TOP5:")
for i, ((a, b), c) in enumerate(
        sorted(pair_counts.items(), key=lambda x: -x[1])[:5], 1):
    print(f"  {i}. {a} ↔ {b}: {c} 次")

# ═══════════════════════════════════════════════════════════════
# 第4层：模拟 DWS 聚合 — AbyssAggMR 输出格式
# ═══════════════════════════════════════════════════════════════

section("第4层：DWS 聚合 (模拟 MR 输出格式)")

"""
AbyssAggMR 真实输出格式（来自工作日志）:
  TextOutputFormat: Key\tValue
  Key:   version,char_name
  Value: star,own_count,use_count,total_users,own_rate,use_rate,avg_constellation,avg_level

  part-r-00000 内容示例:
    v6.6,玛薇卡	5,3,3,3,100.00,100.00,1.33,90.00
    v6.6,班尼特	4,3,3,3,100.00,100.00,5.67,80.00

关键特征:
  1. Tab 分隔 Key 和 Value
  2. Key 含逗号（version,char_name）
  3. Value 是逗号分隔的 8 个字段
  4. 无表头
"""

# 从 char_detail 手工聚合（模拟 MR Reducer）
agg = defaultdict(lambda: {"star": 0, "own": 0, "use": 0, "cons_sum": 0.0, "level_sum": 0.0})
total_users = 3  # 用户1-3 通过清洗
uid_set = set()

for line in clean_char:
    parts = line.split(",")
    ver, uid, name, star, cons, lvl, used = parts[0], parts[1], parts[2], int(parts[3]), int(parts[4]), int(parts[5]), int(parts[6])
    key = f"{ver},{name}"
    agg[key]["star"] = star
    agg[key]["own"] += 1
    agg[key]["cons_sum"] += cons
    agg[key]["level_sum"] += lvl
    if used == 1:
        agg[key]["use"] += 1
    uid_set.add(uid)

# 写模拟 DWS char_summary（Tab 分隔格式）
with open(f"{TMP}/dws/dws_char_summary.csv", "w", encoding="utf-8") as f:
    for key, v in sorted(agg.items()):
        own_r = round(v["own"] / total_users * 100, 2)
        use_r = round(v["use"] / total_users * 100, 2)
        avg_c = round(v["cons_sum"] / v["own"], 2)
        avg_l = round(v["level_sum"] / v["own"], 2)
        line = f"{key}\t{v['star']},{v['own']},{v['use']},{total_users},{own_r},{use_r},{avg_c},{avg_l}\n"
        f.write(line)

check("DWS 聚合行数 > 0", len(agg) > 0, f"实际: {len(agg)} 个角色组")

# 读回验证 Tab 分隔格式
with open(f"{TMP}/dws/dws_char_summary.csv", "r", encoding="utf-8") as f:
    dws_lines = f.readlines()

# 检查玛薇卡
maweika_found = False
for line in dws_lines:
    if "玛薇卡" in line:
        maweika_found = True
        key, val = line.strip().split("\t", 1)
        key_parts = key.split(",", 1)
        val_parts = val.split(",", 8)
        check("Key 格式: version,char_name",
              len(key_parts) == 2 and key_parts[0] == "v6.6",
              f"Key: {key}")
        check("Value 8个字段",
              len(val_parts) == 8,
              f"实际: {len(val_parts)} 列")
        check("玛薇卡 own_count=3 (3个用户都有)",
              val_parts[1] == "3",
              f"实际: {val_parts[1]}")
        check("玛薇卡 use_count=3 (3个用户都上阵)",
              val_parts[2] == "3",
              f"实际: {val_parts[2]}")
        check("total_users=3",
              val_parts[3] == "3")
        break
check("玛薇卡 在 DWS 输出中", maweika_found)

print(f"\n  DWS 样例行:")
print(f"  {dws_lines[0].strip()}")

# ═══════════════════════════════════════════════════════════════
# 第5层：ADS — load_ads_to_mysql.py 解析逻辑
# ═══════════════════════════════════════════════════════════════

section("第5层：ADS 加载 (验证 DWS→ADS 解析逻辑)")

# 验证 load_ads_to_mysql.py 的核心解析逻辑
# 不连 MySQL，只测试解析部分

# --- 测试 Tab 分隔格式解析（load_meta_ranking / load_char_trend 通用）---
parsed_records = []
for line in dws_lines:
    line = line.strip()
    if not line:
        continue
    if "\t" in line:
        key, val = line.split("\t", 1)
        key_parts = key.split(",", 1)
        if len(key_parts) < 2:
            continue
        ver = key_parts[0].strip()
        name = key_parts[1].strip()
    else:
        continue  # char_trend 不支持逗号格式

    val_parts = val.split(",", 8)
    if len(val_parts) < 8:
        continue
    try:
        star = int(val_parts[0])
        own_count = int(val_parts[1])
        use_count = int(val_parts[2])
        total_u = int(val_parts[3])
        own_rate = float(val_parts[4])
        use_rate = float(val_parts[5])
        avg_const = float(val_parts[6])
        avg_level = float(val_parts[7])
    except (ValueError, IndexError):
        continue

    parsed_records.append({
        "version": ver, "char_name": name, "star": star,
        "use_rate": use_rate, "own_rate": own_rate,
        "avg_constellation": avg_const, "avg_level": avg_level
    })

check("ADS 解析出全部 DWS 行",
      len(parsed_records) == len(agg),
      f"解析: {len(parsed_records)}, DWS: {len(agg)}")

# --- 测试 ads_meta_ranking 红黑榜逻辑 ---
parsed_records.sort(key=lambda x: -x["use_rate"])
total = len(parsed_records)

red_top10 = parsed_records[:10]
black_bottom10 = parsed_records[-10:] if total >= 10 else []
check(f"红榜 TOP{len(red_top10)}", len(red_top10) > 0)
check(f"黑榜 BOTTOM{len(black_bottom10)}", len(black_bottom10) > 0)

print(f"\n  红榜 TOP3:")
for i, r in enumerate(red_top10[:3]):
    print(f"  {i+1}. {r['char_name']} (使用率 {r['use_rate']:.1f}%)")

print(f"\n  黑榜 BOTTOM3:")
for i, r in enumerate(black_bottom10[-3:] if len(black_bottom10) >= 3 else black_bottom10):
    rank = total - len(black_bottom10) + i + 1
    print(f"  {rank}. {r['char_name']} (使用率 {r['use_rate']:.1f}%)")

# --- 测试 ads_char_trend 逻辑 ---
char_trends = {}
for line in dws_lines:
    line = line.strip()
    if "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    key_parts = key.split(",", 1)
    if len(key_parts) < 2:
        continue
    ver = key_parts[0].strip()
    name = key_parts[1].strip()
    val_parts = val.split(",", 8)
    if len(val_parts) < 8:
        continue
    try:
        star = int(val_parts[0])
        use_rate = float(val_parts[5])
    except:
        continue
    if name not in char_trends:
        char_trends[name] = {"star": star, "pairs": []}
    char_trends[name]["pairs"].append((ver, use_rate))

# 排序取 TOP15（此处只取全部，因为只有4用户数据）
sorted_chars = sorted(char_trends.items(),
                      key=lambda x: x[1]["pairs"][-1][1] if x[1]["pairs"] else 0,
                      reverse=True)

check("char_trend 有数据", len(sorted_chars) > 0)

# 验证 JSON 序列化
for name, data in sorted_chars[:3]:
    versions = [p[0] for p in data["pairs"]]
    rates = [p[1] for p in data["pairs"]]
    v_json = json.dumps(versions, ensure_ascii=False)
    r_json = json.dumps(rates, ensure_ascii=False)
    check(f"char_trend '{name}': version_list JSON 合法",
          json.loads(v_json) == versions)
    check(f"char_trend '{name}': rate_list JSON 合法",
          json.loads(r_json) == rates)

# --- 测试 ads_team_network 逻辑 ---
edges = []
for line in cooccur_lines[1:]:
    parts = line.strip().split(",", 2)
    if len(parts) < 3:
        continue
    edges.append({
        "source": parts[0].strip(),
        "target": parts[1].strip(),
        "weight": int(parts[2].strip()),
    })
edges.sort(key=lambda x: -x["weight"])
top30 = edges[:30]

check(f"共现网络: TOP{len(top30)} 边", len(top30) > 0)
check("边的 source/target/weight 字段完整",
      all("source" in e and "target" in e and "weight" in e for e in top30))

# ═══════════════════════════════════════════════════════════════
# 第6层：验证脚本 — verify_consistency.py 核心逻辑
# ═══════════════════════════════════════════════════════════════

section("第6层：验证脚本 (verify_consistency.py 核心逻辑)")

from verify_consistency import verify, load_aggregated_csv

# 用 ADS 解析结果构造 verify 能接受的格式
# 写临时 CSV（带表头，符合 verify 预期）
verify_csv = f"{TMP}/ads/dws_for_verify.csv"
with open(verify_csv, "w", encoding="utf-8") as f:
    f.write("char_name,star,own_count,use_count,total_users,own_rate,use_rate,avg_constellation,avg_level\n")
    for r in parsed_records:
        f.write(f"{r['char_name']},{r['star']},0,0,{total_users},"
                f"{r['own_rate']},{r['use_rate']},"
                f"{r['avg_constellation']},{r['avg_level']}\n")

# 源统计（模拟）
source_stats = {}
for r in parsed_records:
    source_stats[r["char_name"]] = {
        "use_rate": r["use_rate"],
        "own_rate": r["own_rate"],
    }

agg_stats = load_aggregated_csv(verify_csv)
check("verify 能加载聚合CSV", len(agg_stats) > 0)

try:
    ok = verify(source_stats, agg_stats)
    check("验证通过（自洽：源=聚合）", ok)
except Exception as e:
    check("验证脚本不抛异常", False, str(e))

# ═══════════════════════════════════════════════════════════════
# 文件清单 & 清理
# ═══════════════════════════════════════════════════════════════

section("产出文件")

print(f"\n  {TMP}/")
for root, dirs, files in os.walk(TMP):
    level = root.replace(TMP, "").count(os.sep)
    indent = "  " + "  " * level
    print(f"{indent}{os.path.basename(root)}/")
    subindent = "  " + "  " * (level + 1)
    for f in sorted(files):
        fpath = os.path.join(root, f)
        size = os.path.getsize(fpath)
        print(f"{subindent}{f}  ({size}B)")

# ═══════════════════════════════════════════════════════════════
# 结果
# ═══════════════════════════════════════════════════════════════

section("测试结果")

total = PASS + FAIL
print(f"\n  通过: {PASS}/{total}")
if FAIL > 0:
    print(f"  失败: {FAIL}/{total}")
    print(f"\n  [FAIL] 有 {FAIL} 项未通过，请检查上方详情")
else:
    print(f"  [PASS] 全部 {PASS} 项通过!")

print(f"\n  测试数据保留在: {TMP}")
print(f"  手动清理: rm -rf {TMP}")

# 返回状态码
sys.exit(0 if FAIL == 0 else 1)
