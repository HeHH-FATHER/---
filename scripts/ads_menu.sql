-- ADS 数据管理菜单 — 挂在数据管理(2018)下
-- mysql -u root -p123456 --default-character-set=utf8mb4 abyss_db < ads_menu.sql

USE abyss_db;

-- ===== 使用率排名 =====
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('使用率排名', '2018', '4', 'meta', 'ruoyi-data/meta/index', 1, 0, 'C', '0', '0', 'ruoyi-data:meta:list', '#', 'admin', sysdate(), '', null, '使用率排名菜单');
SELECT @parentId := LAST_INSERT_ID();
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('使用率排名查询', @parentId, '1',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:meta:query', '#', 'admin', sysdate(), '', null, ''),
('使用率排名新增', @parentId, '2',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:meta:add',   '#', 'admin', sysdate(), '', null, ''),
('使用率排名修改', @parentId, '3',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:meta:edit',  '#', 'admin', sysdate(), '', null, ''),
('使用率排名删除', @parentId, '4',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:meta:remove','#', 'admin', sysdate(), '', null, ''),
('使用率排名导出', @parentId, '5',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:meta:export','#', 'admin', sysdate(), '', null, '');

-- ===== 角色趋势 =====
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('角色趋势', '2018', '5', 'trend', 'ruoyi-data/trend/index', 1, 0, 'C', '0', '0', 'ruoyi-data:trend:list', '#', 'admin', sysdate(), '', null, '角色趋势菜单');
SELECT @parentId := LAST_INSERT_ID();
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('角色趋势查询', @parentId, '1',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:trend:query', '#', 'admin', sysdate(), '', null, ''),
('角色趋势新增', @parentId, '2',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:trend:add',   '#', 'admin', sysdate(), '', null, ''),
('角色趋势修改', @parentId, '3',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:trend:edit',  '#', 'admin', sysdate(), '', null, ''),
('角色趋势删除', @parentId, '4',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:trend:remove','#', 'admin', sysdate(), '', null, ''),
('角色趋势导出', @parentId, '5',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:trend:export','#', 'admin', sysdate(), '', null, '');

-- ===== 配队共现 =====
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('配队共现', '2018', '6', 'team', 'ruoyi-data/team/index', 1, 0, 'C', '0', '0', 'ruoyi-data:team:list', '#', 'admin', sysdate(), '', null, '配队共现菜单');
SELECT @parentId := LAST_INSERT_ID();
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('配队共现查询', @parentId, '1',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:team:query', '#', 'admin', sysdate(), '', null, ''),
('配队共现新增', @parentId, '2',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:team:add',   '#', 'admin', sysdate(), '', null, ''),
('配队共现修改', @parentId, '3',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:team:edit',  '#', 'admin', sysdate(), '', null, ''),
('配队共现删除', @parentId, '4',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:team:remove','#', 'admin', sysdate(), '', null, ''),
('配队共现导出', @parentId, '5',  '#', '', 1, 0, 'F', '0', '0', 'ruoyi-data:team:export','#', 'admin', sysdate(), '', null, '');

-- ===== 数据分析(2048)下新增卡池分析 =====
insert into sys_menu (menu_name, parent_id, order_num, path, component, is_frame, is_cache, menu_type, visible, status, perms, icon, create_by, create_time, update_by, update_time, remark)
values('卡池抽取分析', '2048', '3', 'analysis/gacha', 'analysis/gacha/index', 1, 0, 'C', '0', '0', 'analysis:gacha:list', '#', 'admin', sysdate(), '', null, '卡池抽取分析菜单');

SELECT menu_name, parent_id, order_num FROM sys_menu WHERE parent_id = 2018;
