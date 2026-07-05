#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
逐风数据洞察平台 — ODS 层 数据预处理
═══════════════════════════════════════════════════════════════
功能: 读取 Abyss-Record-Generator 生成的每用户 JSON 文件，
      合并 BOX + 战绩 → 版本化 JSONL → 上传 HDFS

层级: ODS（HDFS）— 原始数据
下游: DWD 层 AbyssCleanMR

输入: 每个用户 2 个 JSON
  {uid}_char_box.json     — 角色BOX {"uid":"...","characters":[...]}
  {uid}_abyss_record.json — 深渊战绩 {"uid":"...","teams":[...]}

输出: 单文件 JSONL（一行一个用户，含版本标记）
  {"uid":"...","version":"v6.6","box":{...},"record":{...}}

用法:
  python preprocess_abyss.py <生成器输出目录> --version v6.6 [--upload] [--hdfs-dir /data/abyss/ods/]
═══════════════════════════════════════════════════════════════
"""

import json
import os
import sys
import glob
import subprocess
import argparse
from datetime import datetime


# ═══════════════════════════════════════════════
# 扫描用户文件
# ═══════════════════════════════════════════════

def find_user_files(input_dir):
    """扫描目录，建立 uid → {box, record} 映射"""
    users = {}
    box_pattern = os.path.join(input_dir, "*_char_box.json")
    for box_path in sorted(glob.glob(box_pattern)):
        basename = os.path.basename(box_path)
        uid = basename.replace("_char_box.json", "")
        record_path = os.path.join(input_dir, f"{uid}_abyss_record.json")
        users[uid] = {"box": box_path}
        if os.path.exists(record_path):
            users[uid]["record"] = record_path
    return users


# ═══════════════════════════════════════════════
# 合并单用户
# ═══════════════════════════════════════════════

def merge_user(uid, version, box_path, record_path):
    """合并 BOX + 战绩 → 单行 JSON（含版本）"""
    with open(box_path, "r", encoding="utf-8") as f:
        box = json.load(f)
    record = None
    if record_path and os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            record = json.load(f)

    return json.dumps({
        "uid": uid,
        "version": version,
        "box": box,
        "record": record  # 可能为 null（仅BOX无战绩的脏数据）
    }, ensure_ascii=False)


# ═══════════════════════════════════════════════
# 主处理
# ═══════════════════════════════════════════════

def preprocess(input_dir, version, output_path, quiet=False):
    """遍历所有用户，输出单文件 JSONL"""
    users = find_user_files(input_dir)

    if not users:
        print(f"[ERROR] 目录 {input_dir} 中未找到 *_char_box.json")
        return None

    total = len(users)
    missing_record = sum(1 for u in users.values() if "record" not in u)

    if not quiet:
        print(f"[ODS] 扫描到 {total} 个用户")
        if missing_record:
            print(f"[ODS] ⚠ 其中 {missing_record} 个缺少战绩文件（将标记为 record=null）")

    # 确保输出目录存在
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    written = 0
    skipped = 0
    report_interval = max(1, total // 10)

    with open(output_path, "w", encoding="utf-8") as out:
        for uid, paths in sorted(users.items()):
            try:
                line = merge_user(uid, version, paths["box"], paths.get("record"))
                out.write(line + "\n")
                written += 1
            except Exception as e:
                if not quiet:
                    print(f"  [SKIP] {uid}: {e}")
                skipped += 1

            if not quiet and (written + skipped) % report_interval == 0:
                print(f"  进度: {written + skipped}/{total}")

    file_size_mb = os.path.getsize(output_path) / 1024 / 1024

    if not quiet:
        print(f"\n[ODS] ✓ 预处理完成")
        print(f"  输出: {output_path}")
        print(f"  用户数: {written} (跳过: {skipped})")
        print(f"  文件大小: {file_size_mb:.1f} MB")
        print(f"  版本: {version}")

    return output_path


# ═══════════════════════════════════════════════
# HDFS 上传
# ═══════════════════════════════════════════════

def upload_to_hdfs(local_path, hdfs_dir):
    """上传 JSONL → HDFS ODS 层"""
    filename = os.path.basename(local_path)
    hdfs_path = hdfs_dir.rstrip("/") + "/" + filename

    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
            check=False, capture_output=True
        )
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
            check=False, capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"[HDFS] ✓ {filename} → {hdfs_path}")
            return hdfs_path
        else:
            print(f"[HDFS] ✗ 上传失败: {result.stderr.strip()}")
            return None
    except FileNotFoundError:
        print(f"[HDFS] ⚠ hdfs 命令不可用（文件已保存在本地: {local_path}）")
        return None


# ═══════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="逐风平台 — ODS层 数据预处理 (JSON → JSONL → HDFS)"
    )
    parser.add_argument("input_dir", help="生成器输出目录（含 *_char_box.json）")
    parser.add_argument("--version", "-v", required=True,
                        help="数据版本，如 v6.6(第一期)")
    parser.add_argument("--output", "-o", default=None,
                        help="本地 JSONL 输出路径（默认: <input_dir>/../ods/abyss_<version>_<ts>.jsonl）")
    parser.add_argument("--upload", "-u", action="store_true",
                        help="上传到 HDFS")
    parser.add_argument("--hdfs-dir", default="/data/abyss/ods/",
                        help="HDFS ODS 层目录（默认: /data/abyss/ods/）")
    parser.add_argument("--quiet", "-q", action="store_true")

    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        print(f"[ERROR] 输入目录不存在: {input_dir}")
        sys.exit(1)

    # 输出路径
    if args.output:
        output_path = args.output
    else:
        output_dir = os.path.join(os.path.dirname(input_dir), "ods")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_ver = args.version.replace("(", "").replace(")", "").replace("/", "_")
        output_path = os.path.join(output_dir, f"abyss_{safe_ver}_{ts}.jsonl")

    if not args.quiet:
        print("=" * 60)
        print("逐风数据洞察平台 — ODS 层 数据预处理")
        print("=" * 60)
        print(f"输入: {input_dir}")
        print(f"版本: {args.version}")
        print(f"输出: {output_path}")

    # 执行
    result = preprocess(input_dir, args.version, output_path, args.quiet)
    if not result:
        sys.exit(1)

    # 上传 HDFS
    if args.upload:
        hdfs_path = upload_to_hdfs(result, args.hdfs_dir)
        if hdfs_path:
            print(f"\n[ODS] ✓ 数据已就绪: {hdfs_path}")
            print(f"[ODS] ↓ 下游: DWD 层 AbyssCleanMR")

    if not args.quiet:
        print("=" * 60)

    return result


if __name__ == "__main__":
    main()
