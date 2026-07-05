-- ============================================================
-- 逐风数据洞察平台 — MySQL 建表语句（v4.2 终稿）
-- 数据库: abyss_db
--
-- 分层策略:
--   ODS 层 → HDFS（原始文件，不进MySQL）
--   DWD 层 → MySQL（Spark 从 HDFS 读取清洗结果灌入）
--   DWS 层 → MySQL（Spark 聚合计算写入）
--   ADS 层 → MySQL（API 读给大屏）
--   维表   → MySQL（角色/武器/卡池字典，Spark JOIN 用）
--   实时落地 → MySQL（Streaming 快照，供验证对比）
-- ============================================================

CREATE DATABASE IF NOT EXISTS abyss_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE abyss_db;

-- ============================================================
-- 一、维表（字典类 — 从 CSV 直接导入，查字典用）
-- ============================================================

-- 1.1 角色字典
DROP TABLE IF EXISTS dim_role;
CREATE TABLE dim_role (
    id INT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    star INT COMMENT '星级',
    avatar VARCHAR(500) COMMENT '头像URL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_name (char_name)
) ENGINE=InnoDB COMMENT='角色维表';

-- 1.2 武器字典
DROP TABLE IF EXISTS dim_weapon;
CREATE TABLE dim_weapon (
    id INT AUTO_INCREMENT PRIMARY KEY,
    weapon_name VARCHAR(100) NOT NULL COMMENT '武器名称',
    avatar VARCHAR(500) COMMENT '图标URL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_name (weapon_name)
) ENGINE=InnoDB COMMENT='武器维表';

-- 1.3 卡池历史
DROP TABLE IF EXISTS dim_banner;
CREATE TABLE dim_banner (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version_name VARCHAR(50) NOT NULL COMMENT '卡池版本',
    start_time DATE COMMENT '开始日期',
    end_time DATE COMMENT '结束日期',
    char_name VARCHAR(50) NOT NULL COMMENT 'UP角色名',
    pull_count INT COMMENT '抽取次数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_version (version_name),
    INDEX idx_char (char_name)
) ENGINE=InnoDB COMMENT='卡池历史维表';

-- ============================================================
-- 二、DWD 层（清洗后明细 — Spark 从 HDFS /dwd/ 读入）
-- ============================================================

-- 2.1 角色使用率明细
DROP TABLE IF EXISTS dwd_char_usage;
CREATE TABLE dwd_char_usage (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    version_name VARCHAR(50) NOT NULL COMMENT '版本名称',
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    star INT COMMENT '星级',
    use_count INT COMMENT '使用次数',
    own_count INT COMMENT '持有次数',
    use_rate DECIMAL(5,2) COMMENT '使用率(%)',
    own_rate DECIMAL(5,2) COMMENT '持有率(%)',
    tier VARCHAR(10) COMMENT '梯度',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_char_ver (char_name, version_name),
    INDEX idx_version (version_name)
) ENGINE=InnoDB COMMENT='角色使用率明细';

-- 2.2 配队明细（队伍拆分为角色级）
DROP TABLE IF EXISTS dwd_team_detail;
CREATE TABLE dwd_team_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    version_name VARCHAR(50) NOT NULL COMMENT '版本名称',
    team_comp VARCHAR(200) NOT NULL COMMENT '完整配队',
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    position INT COMMENT '队伍中位置1-4',
    use_count INT COMMENT '使用次数',
    use_rate DECIMAL(5,2) COMMENT '使用率(%)',
    attend_rate DECIMAL(5,2) COMMENT '登场率(%)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_char (char_name),
    INDEX idx_team (team_comp)
) ENGINE=InnoDB COMMENT='配队明细';

-- ============================================================
-- 三、DWS 层（汇总聚合 — Spark 计算写入）
-- ============================================================

-- 3.1 角色使用率汇总
DROP TABLE IF EXISTS dws_char_usage_avg;
CREATE TABLE dws_char_usage_avg (
    id INT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    star INT COMMENT '星级',
    avg_use_rate DECIMAL(5,2) COMMENT '平均使用率',
    max_use_rate DECIMAL(5,2) COMMENT '最高使用率',
    min_use_rate DECIMAL(5,2) COMMENT '最低使用率',
    version_count INT COMMENT '出现版本数',
    trend_direction VARCHAR(10) COMMENT '趋势: 上升/下降/稳定',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_char (char_name)
) ENGINE=InnoDB COMMENT='角色使用率汇总';

-- 3.2 角色使用率趋势（LAG窗口环比）
DROP TABLE IF EXISTS dws_char_usage_trend;
CREATE TABLE dws_char_usage_trend (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    version_name VARCHAR(50) NOT NULL COMMENT '版本名称',
    use_rate DECIMAL(5,2) COMMENT '使用率',
    prev_use_rate DECIMAL(5,2) COMMENT '上期使用率',
    change_pct DECIMAL(5,2) COMMENT '环比变化',
    rank_current INT COMMENT '当期排名',
    rank_prev INT COMMENT '上期排名',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_char (char_name),
    INDEX idx_version (version_name)
) ENGINE=InnoDB COMMENT='角色使用率趋势';

-- 3.3 配队频繁项（FP-Growth 挖掘）
DROP TABLE IF EXISTS dws_team_freq_items;
CREATE TABLE dws_team_freq_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    char_pair VARCHAR(100) NOT NULL COMMENT '角色对',
    cooccur_count INT COMMENT '共现次数',
    support DECIMAL(5,4) COMMENT '支持度',
    confidence DECIMAL(5,4) COMMENT '置信度',
    lift DECIMAL(5,2) COMMENT '提升度',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_lift (lift)
) ENGINE=InnoDB COMMENT='配队频繁项';

-- ============================================================
-- 四、ADS 层（大屏消费 — API 直读）
-- ============================================================

-- 4.1 使用率红黑榜
DROP TABLE IF EXISTS ads_meta_ranking;
CREATE TABLE ads_meta_ranking (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rank_num INT COMMENT '排名',
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    star INT COMMENT '星级',
    avatar VARCHAR(500) COMMENT '头像URL',
    use_rate DECIMAL(5,2) COMMENT '使用率',
    own_rate DECIMAL(5,2) COMMENT '持有率',
    list_type VARCHAR(10) NOT NULL COMMENT 'red红榜/black黑榜',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_type_rank (list_type, rank_num)
) ENGINE=InnoDB COMMENT='使用率红黑榜';

-- 4.2 角色长青榜
DROP TABLE IF EXISTS ads_char_trend;
CREATE TABLE ads_char_trend (
    id INT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    star INT COMMENT '星级',
    avatar VARCHAR(500) COMMENT '头像URL',
    version_list TEXT COMMENT '版本列表JSON',
    rate_list TEXT COMMENT '使用率列表JSON',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_char (char_name)
) ENGINE=InnoDB COMMENT='角色长青榜';

-- 4.3 配队共现网络
DROP TABLE IF EXISTS ads_team_network;
CREATE TABLE ads_team_network (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL COMMENT '源角色',
    target_name VARCHAR(50) NOT NULL COMMENT '目标角色',
    source_avatar VARCHAR(500) COMMENT '源头像',
    target_avatar VARCHAR(500) COMMENT '目标头像',
    weight INT COMMENT '共现权重',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_src (source_name),
    INDEX idx_tgt (target_name)
) ENGINE=InnoDB COMMENT='配队共现网络';

-- ============================================================
-- 五、实时落地表（Streaming/MR 结果落地，供验证对比）
-- ============================================================

-- 5.1 抽卡聚合结果（MR 验证）
DROP TABLE IF EXISTS rt_gacha_result;
CREATE TABLE rt_gacha_result (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    pull_count BIGINT COMMENT 'MR聚合抽取次数',
    rank_num INT COMMENT '排名',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rank (rank_num)
) ENGINE=InnoDB COMMENT='抽卡聚合结果';

-- 5.2 练度快照（Streaming 落地）
DROP TABLE IF EXISTS rt_build_snapshot;
CREATE TABLE rt_build_snapshot (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    char_name VARCHAR(50) NOT NULL COMMENT '角色名称',
    window_time DATETIME COMMENT '窗口时间',
    avg_constellation DECIMAL(3,2) COMMENT '平均命座',
    avg_damage BIGINT COMMENT '平均伤害',
    top_weapon VARCHAR(100) COMMENT '使用率最高武器',
    top_artifact VARCHAR(100) COMMENT '使用率最高圣遗物',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_char_time (char_name, window_time)
) ENGINE=InnoDB COMMENT='练度流处理快照';

-- ============================================================
-- 验证
-- ============================================================
SELECT
    '维表' AS layer, COUNT(*) AS tables
FROM information_schema.tables
WHERE table_schema='abyss_db' AND table_name LIKE 'dim_%'
UNION ALL
SELECT 'DWD', COUNT(*) FROM information_schema.tables WHERE table_schema='abyss_db' AND table_name LIKE 'dwd_%'
UNION ALL
SELECT 'DWS', COUNT(*) FROM information_schema.tables WHERE table_schema='abyss_db' AND table_name LIKE 'dws_%'
UNION ALL
SELECT 'ADS', COUNT(*) FROM information_schema.tables WHERE table_schema='abyss_db' AND table_name LIKE 'ads_%'
UNION ALL
SELECT '实时落地', COUNT(*) FROM information_schema.tables WHERE table_schema='abyss_db' AND table_name LIKE 'rt_%';
