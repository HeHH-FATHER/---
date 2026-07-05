"""
深渊通关记录生成器
基于提瓦特小助手真实数据分布生成海量模拟数据
输出: CSV → HDFS /data/abyss/
"""
import json
import random
import csv
import os
import math
from datetime import datetime, timedelta

# ============================================================
# 1. 加载真实数据模板
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '提瓦特数据')

def load_json(filename):
    with open(os.path.join(DATA_DIR, filename), 'r', encoding='utf-8') as f:
        return json.load(f)

print("加载数据模板...")
char_usage = load_json('深渊角色使用率.json')      # 115角色使用率
role_avg   = load_json('角色练度统计.json')         # 122角色武器/圣遗物/命座/伤害
vote_rank  = load_json('角色满意度排行.json')       # 121角色满意度(没用但保留)
team_data  = load_json('队伍配置汇总.json')         # 14期TOP配队

# ============================================================
# 2. 构建生成参数
# ============================================================

# 角色使用率 → 加权概率
char_names = [c['name'] for c in char_usage]
char_probs = [c['use_rate'] / 100 for c in char_usage]  # 独立伯努利概率

# 每个角色的武器分布 {角色名: [(武器名, 概率), ...]}
weapon_dist = {}
for r in role_avg:
    wlist = r.get('weapons', [])
    weapons = [(w['name'], w['rate'] / 100) for w in wlist]
    weapon_dist[r['role']] = weapons if weapons else [('无数据', 1.0)]

# 每个角色的圣遗物分布
artifact_dist = {}
for r in role_avg:
    arts = [(a['name'], a['rate'] / 100) for a in r.get('artifact_sets', [])]
    artifact_dist[r['role']] = arts if arts else [('无数据', 1.0)]

# 每个角色的命座分布
const_dist = {}
for r in role_avg:
    cd = r.get('constellation_dist', {})
    c_probs = [cd.get(f'c{i}', 0) for i in range(7)]
    total = sum(c_probs)
    const_dist[r['role']] = [p/total for p in c_probs] if total > 0 else [1,0,0,0,0,0,0]

# 伤害参数
damage_params = {}
for r in role_avg:
    damage_params[r['role']] = {
        'mean_damage': r['avg_damage'],
        'damage_name': r.get('damage_type', '伤害')
    }

# 配队种子（真实TOP配队）
seed_teams = []
for ver_id, info in team_data.items():
    roles = [m['role'] for m in info['role_list']]
    seed_teams.append(roles)

print(f"  角色数: {len(char_names)}")
print(f"  武器分布: {len(weapon_dist)} 个角色")
print(f"  圣遗物分布: {len(artifact_dist)} 个角色")
print(f"  配队种子: {len(seed_teams)} 个")

# ============================================================
# 3. 生成函数
# ============================================================

ABYSS_BUFFS = [
    "渊月祝福·绽放之月", "渊月祝福·激化之月", "渊月祝福·蒸发之月",
    "渊月祝福·冰封之月", "渊月祝福·雷暴之月", "渊月祝福·岩固之月"
]

def generate_uid():
    """生成9位UID"""
    return random.randint(100000000, 999999999)

def generate_team():
    """
    按真实使用率生成一支队伍的角色列表
    60%概率使用种子配队+变异, 40%完全随机
    """
    if random.random() < 0.6 and seed_teams:
        # 基于真实配队微调
        base = random.choice(seed_teams).copy()
        # 随机替换1-2个角色
        if random.random() < 0.3:
            replace_count = random.randint(1, 2)
            for _ in range(replace_count):
                idx = random.randint(0, 3)
                # 按使用率选新角色
                new_char = random.choices(char_names, weights=char_probs)[0]
                base[idx] = new_char
        return base
    else:
        # 独立伯努利：每个角色按use_rate出现
        appeared = [c for c, p in zip(char_names, char_probs) if random.random() < p]
        if len(appeared) >= 4:
            return random.sample(appeared, 4)
        else:
            return random.choices(char_names, weights=char_probs, k=4)

def generate_character_detail(name):
    """为一个角色生成武器/圣遗物/命座"""
    # 武器
    weapons = weapon_dist.get(name, [('无数据', 1.0)])
    w_names = [w[0] for w in weapons]
    w_probs = [w[1] for w in weapons]
    weapon = random.choices(w_names, weights=w_probs)[0]

    # 圣遗物
    arts = artifact_dist.get(name, [('无数据', 1.0)])
    a_names = [a[0] for a in arts]
    a_probs = [a[1] for a in arts]
    artifact = random.choices(a_names, weights=a_probs)[0]

    # 命座
    c_probs = const_dist.get(name, [1,0,0,0,0,0,0])
    constellation = random.choices(range(7), weights=c_probs)[0]

    # 伤害
    dmg_info = damage_params.get(name, {'mean_damage': 10000, 'damage_name': '未知'})
    # 对数正态分布，mu = ln(mean), sigma = 0.5
    mu = math.log(max(dmg_info['mean_damage'], 100))
    damage = int(random.lognormvariate(mu, 0.5))

    return weapon, artifact, constellation, damage, dmg_info['damage_name']

def generate_record(version_start, version_end):
    """生成一条完整的深渊通关记录"""
    uid = generate_uid()
    team = generate_team()
    floor = random.choices([9,10,11,12], weights=[0.1, 0.15, 0.25, 0.5])[0]

    # 满星率100%，所以大部分是9星
    stars = random.choices([9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
                          weights=[95, 2, 1, 0.5, 0.3, 0.2, 0.2, 0.2, 0.3, 0.3])[0]

    # 通关耗时：正态分布，均值90秒，标准差30秒
    clear_time = max(10, int(random.gauss(90, 30)))

    buff = random.choice(ABYSS_BUFFS)
    ts = version_start + timedelta(
        seconds=random.randint(0, int((version_end - version_start).total_seconds()))
    )

    # 生成队伍详情
    team_detail = []
    for name in team:
        weapon, artifact, const, dmg, dmg_type = generate_character_detail(name)
        team_detail.append({
            'character': name,
            'weapon': weapon,
            'artifact': artifact,
            'constellation': const,
            'avg_damage': dmg,
            'damage_type': dmg_type
        })

    return {
        'uid': uid,
        'team': ','.join(team),
        'team_detail': json.dumps(team_detail, ensure_ascii=False),
        'floor': floor,
        'stars': stars,
        'clear_time': clear_time,
        'buff': buff,
        'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S')
    }

# ============================================================
# 4. 批量生成 & 输出
# ============================================================

def generate_dataset(num_records=1_000_000, output_dir=None):
    """批量生成深渊记录"""
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)

    # 模拟60个版本周期
    # 每期21天，从2024-08-28(v3.0)开始
    base_date = datetime(2024, 8, 28)
    period_days = 21

    output_file = os.path.join(output_dir, f'abyss_records_{num_records}.csv')
    print(f"\n生成 {num_records:,} 条深渊记录...")
    print(f"输出: {output_file}")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'uid', 'team', 'team_detail', 'floor', 'stars',
            'clear_time', 'buff', 'timestamp'
        ])
        writer.writeheader()

        for i in range(num_records):
            # 随机分配到一个版本周期
            version_idx = random.randint(0, 59)
            v_start = base_date + timedelta(days=version_idx * period_days)
            v_end = v_start + timedelta(days=period_days)

            record = generate_record(v_start, v_end)
            writer.writerow(record)

            if (i + 1) % 200000 == 0:
                print(f"  已生成 {i+1:,} / {num_records:,}")

    print("完成!")
    return output_file

if __name__ == '__main__':
    # 默认生成100万条，约150MB
    generate_dataset(num_records=1_000_000)
