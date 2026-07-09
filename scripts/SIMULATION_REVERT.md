# 管道仿真器 — 还原说明

## 仿真脚本
`scripts/simulate_pipeline.py` — 本地 Python 脚本，直连 Middleware Redis/MySQL，模拟实时链路。

## 明天还原步骤

### 1. 停止仿真
```bash
# Windows 本地终端
pkill -f simulate_pipeline
# 或者 Ctrl+C
```

### 2. 启动真实管道（在各自节点上）
```bash
# master0: 启动生成器
ssh root@100.84.184.73
cd /root/abyss-pipeline
nohup bash scripts/run_gen_loop.sh > /tmp/gen_loop.log 2>&1 &

# Middleware: 启动消费端
ssh root@100.103.177.85
cd /root/abyss-pipeline
nohup python3 scripts/realtime_consumer.py > /tmp/consumer.log 2>&1 &
nohup python3 scripts/hot_char_aggregator.py > /tmp/hot_char.log 2>&1 &
```

### 3. 验证切换
```bash
# Kafka 消息确认
ssh root@Middleware '/root/kafka2/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic build-v2 --time -1'

# Redis 数据确认
ssh root@Middleware 'redis-cli GET gacha:pull_count'
ssh root@Middleware 'redis-cli GET build:hot_chars'
```

### 4. 修改过的文件
- `scripts/hot_char_aggregator.py` — **已修改**（加了 MySQL 落库），⚠️ **必须 scp 到 Middleware 覆盖旧版**
  ```bash
  scp scripts/hot_char_aggregator.py root@100.103.177.85:/root/abyss-pipeline/scripts/
  ```
- `scripts/simulate_pipeline.py` — 仿真脚本（Windows本地，直接 `taskkill //F //IM python3.11.exe` 停掉）
- Middleware MySQL `rt_build_hot_char_detail` 表 — 集群恢复后 hot_char_aggregator.py 正常写入，不动
- RuoYi 项目新增文件（正常使用，不用回退）：
  - `AnalysisController.java` — 分析API
  - `analysis/char.js`, `team.js`, `gacha.js` — 前端API
  - `analysis/char/`, `team/`, `gacha/`, `build/` — Vue页面
  - `public/analysis/` — 深色风格HTML + abyss_api.js + gacha_api.js + build_api.js

### 5. 无需改动的部分
- Redis key 格式完全一致，切换无缝
- RuoYi 后端代码未做任何与集群/仿真相关的修改
- 前端代码未动
