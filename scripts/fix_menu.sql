-- 数据中台菜单重构：大屏、分析、质量 三个一级菜单
-- mysql -u root -p123456 --default-character-set=utf8mb4 < fix_menu.sql

USE abyss_db;

-- ===== 1. 清理旧的 =====
-- 删掉 2037 下所有子菜单
DELETE FROM sys_menu WHERE parent_id = 2037;
-- 删掉数据分析相关的一级菜单（如果之前插入过）
DELETE FROM sys_menu WHERE menu_name IN ('数据分析', '数据质量') AND parent_id = 0;

-- ===== 2. 改 2037 为一级菜单「数据大屏」 =====
UPDATE sys_menu SET menu_name = '数据大屏', parent_id = 0, order_num = 3, icon = 'dashboard' WHERE menu_id = 2037;

-- ===== 3. 新建一级菜单「数据分析」=====
INSERT INTO sys_menu (menu_name, parent_id, order_num, path, component, menu_type, icon, status)
VALUES ('数据分析', 0, 4, 'analysis', NULL, 'M', 'chart', '0');
-- 拿到刚插入的 menu_id
SET @analysis_id = LAST_INSERT_ID();

-- ===== 4. 新建一级菜单「数据质量」=====
INSERT INTO sys_menu (menu_name, parent_id, order_num, path, component, menu_type, icon, status)
VALUES ('数据质量', 0, 5, 'quality', NULL, 'M', 'guide', '0');
SET @quality_id = LAST_INSERT_ID();

-- ===== 5. 数据分析下的子菜单 =====
INSERT INTO sys_menu (menu_name, parent_id, order_num, path, component, menu_type, perms, icon, status)
VALUES
('角色使用率分析', @analysis_id, 1, 'analysis/char',  'analysis/char/index',  'C', 'analysis:char:list',  'user',  '0'),
('配队共现分析',   @analysis_id, 2, 'analysis/team', 'analysis/team/index',  'C', 'analysis:team:list',  'share', '0');

-- ===== 6. 数据质量下的子菜单 =====
INSERT INTO sys_menu (menu_name, parent_id, order_num, path, component, menu_type, perms, icon, status)
VALUES
('数据质量报告', @quality_id, 1, 'analysis/quality', 'analysis/quality/index', 'C', 'analysis:quality:list', 'documentation', '0');

-- ===== 7. 数据管理(2018)下新增 ADS 数据浏览 =====
INSERT INTO sys_menu (menu_name, parent_id, order_num, path, component, menu_type, perms, icon, status)
VALUES
('使用率数据', 2018, 4, 'ruoyi-data/meta',  'ruoyi-data/meta/index',  'C', 'ruoyi-data:meta:list',  'chart', '0'),
('角色趋势',   2018, 5, 'ruoyi-data/trend', 'ruoyi-data/trend/index', 'C', 'ruoyi-data:trend:list', 'list',  '0'),
('配队共现',   2018, 6, 'ruoyi-data/team',  'ruoyi-data/team/index',  'C', 'ruoyi-data:team:list',  'tree',  '0');

-- ===== 验证 =====
SELECT menu_id, menu_name, parent_id, order_num FROM sys_menu WHERE menu_id >= 2037 OR parent_id = 2018;
