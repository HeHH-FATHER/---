"""
战斗伤害日志生成器（实时层数据源）
基于提瓦特小助手角色练度数据生成高仿真战斗日志
输出: Kafka Topic battle_log 或 JSON 文件
"""
import json
import random
import math
import time
import os

# ============================================================
# 1. 加载模板数据
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '提瓦特数据')

def load_json(filename):
    with open(os.path.join(DATA_DIR, filename), 'r', encoding='utf-8') as f:
        return json.load(f)

print("加载数据模板...")
role_avg  = load_json('角色练度统计.json')
char_usage = load_json('深渊角色使用率.json')

# 角色使用率 → 出场概率
char_names = [c['name'] for c in char_usage]
char_probs = [c['use_rate'] / 100 for c in char_usage]

# 伤害参数
damage_map = {}
for r in role_avg:
    damage_map[r['role']] = {
        'mean': max(r['avg_damage'], 100),
        'type': r.get('damage_type', '伤害')
    }

# 技能类型权重（按天赋等级分布估算）
SKILL_TYPES = ['Q', 'E', '普攻', 'E', 'Q', '普攻', 'E', 'Q', 'Q', 'E']  # Q和E偏多

# 元素反应类型及比例（手动设定，社区经验值）
REACTIONS = {
    '蒸发': 0.28,
    '激化': 0.18,
    '超载': 0.14,
    '绽放': 0.12,
    '感电': 0.08,
    '超导': 0.06,
    '冻结': 0.06,
    '扩散': 0.05,
    '燃烧': 0.03
}

# 角色→元素映射（原神设定）
CHAR_ELEMENT = {
    '玛薇卡': '火', '班尼特': '火', '香菱': '火', '胡桃': '火', '宵宫': '火',
    '迪卢克': '火', '可莉': '火', '林尼': '火', '阿蕾奇诺': '火', '夏沃蕾': '火',
    '芙宁娜': '水', '那维莱特': '水', '夜兰': '水', '行秋': '水', '达达利亚': '水',
    '珊瑚宫心海': '水', '妮露': '水', '神里绫人': '水', '玛拉妮': '水',
    '雷电将军': '雷', '八重神子': '雷', '菲谢尔': '雷', '久岐忍': '雷',
    '克洛琳德': '雷', '赛诺': '雷', '欧洛伦': '雷', '瓦雷莎': '雷',
    '纳西妲': '草', '艾尔海森': '草', '白术': '草', '艾梅莉埃': '草', '基尼奇': '草',
    '枫原万叶': '风', '流浪者': '风', '魈': '风', '温迪': '风', '闲云': '风', '珐露珊': '风',
    '钟离': '岩', '阿贝多': '岩', '娜维娅': '岩', '千织': '岩', '希诺宁': '岩',
    '茜特菈莉': '冰', '丝柯克': '冰', '甘雨': '冰', '神里绫华': '冰', '申鹤': '冰',
    '莱欧斯利': '冰', '迪奥娜': '冰', '夏洛蒂': '冰', '莱依拉': '冰',
    '兹白': '冰', '尼可': '冰', '洛恩': '冰',
    '杜林': '火', '哥伦比娅': '雷', '菲林斯': '冰', '伊涅芙': '水',
    '菈乌玛': '草', '奈芙尔': '水', '莉奈娅': '火', '法尔伽': '风',
    '叶洛亚': '冰', '布伦妮': '雷', '爱诺': '草', '伊安珊': '雷',
    '爱可菲': '冰',
}
# 补充映射：没有元素信息的角色随机分配
for name in char_names:
    if name not in CHAR_ELEMENT:
        CHAR_ELEMENT[name] = random.choice(['火', '水', '雷', '冰', '风', '岩', '草'])

def get_reaction(char1, char2):
    """根据两个角色元素推断可能的反应"""
    e1 = CHAR_ELEMENT.get(char1, '未知')
    e2 = CHAR_ELEMENT.get(char2, '未知')
    combo = frozenset([e1, e2])

    reaction_map = {
        frozenset(['火', '水']): '蒸发',
        frozenset(['雷', '草']): '激化',
        frozenset(['火', '雷']): '超载',
        frozenset(['水', '草']): '绽放',
        frozenset(['水', '雷']): '感电',
        frozenset(['冰', '雷']): '超导',
        frozenset(['水', '冰']): '冻结',
        frozenset(['风', '火']): '扩散',
        frozenset(['风', '水']): '扩散',
        frozenset(['风', '雷']): '扩散',
        frozenset(['风', '冰']): '扩散',
        frozenset(['火', '草']): '燃烧',
    }
    return reaction_map.get(combo, random.choice(list(REACTIONS.keys())))

# ============================================================
# 2. 生成函数
# ============================================================

def generate_battle_event(chars_in_team):
    """生成一次战斗伤害事件"""
    # 随机选一个角色
    character = random.choices(chars_in_team,
        weights=[char_probs[char_names.index(c)] if c in char_names else 1/len(chars_in_team)
                 for c in chars_in_team])[0]

    skill_type = random.choice(SKILL_TYPES)

    # 伤害: 对数正态分布
    dmg_info = damage_map.get(character, {'mean': 10000, 'type': '伤害'})
    mu = math.log(max(dmg_info['mean'] * (0.3 if skill_type == '普攻' else 1.2 if skill_type == 'Q' else 1.0), 10))
    damage = int(random.lognormvariate(mu, 0.6))

    # 反应: 从队伍中选另一个角色推断反应
    other = random.choice([c for c in chars_in_team if c != character]) if len(chars_in_team) > 1 else character
    reaction = get_reaction(character, other)

    # 如果是核爆
    if damage > 1_000_000:
        reaction = f"{reaction}核爆"

    return {
        'character': character,
        'skill_type': skill_type,
        'reaction': reaction,
        'damage': damage,
        'timestamp': int(time.time() * 1000)  # 毫秒时间戳
    }

# ============================================================
# 3. 输出模式
# ============================================================

def generate_to_file(num_events=100_000, output_path='battle_log.json'):
    """生成战斗日志到JSON文件（测试用）"""
    print(f"生成 {num_events:,} 条战斗日志 → {output_path}")
    team_pool = random.choices(char_names, weights=char_probs, k=4)
    events = [generate_battle_event(team_pool) for _ in range(num_events)]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    print("完成!")
    return output_path

def generate_to_kafka(broker='Middleware:9092', topic='battle_log', rate=1000):
    """
    高频写入Kafka（每秒rate条）
    需要在集群 Middleware 节点运行
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("请先安装: pip install kafka-python")
        return

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print(f"开始写入 Kafka → {broker}, Topic: {topic}, 速率: {rate}条/秒")
    print("Ctrl+C 停止")

    team_pool = random.choices(char_names, weights=char_probs, k=4)
    count = 0
    try:
        while True:
            event = generate_battle_event(team_pool)
            producer.send(topic, value=event)
            count += 1
            if count % rate == 0:
                print(f"  已发送 {count} 条")
                # 每1000条换一支队伍
                team_pool = random.choices(char_names, weights=char_probs, k=4)
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print(f"\n停止。共发送 {count} 条")
    finally:
        producer.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'kafka':
        generate_to_kafka()
    else:
        # 默认生成10万条到本地文件
        generate_to_file(num_events=100_000)
