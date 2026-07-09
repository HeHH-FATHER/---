USE abyss_db;
-- ads_team_usage (23)
UPDATE gen_table_column SET column_comment='配队名' WHERE table_id=23 AND column_name='team_name';
UPDATE gen_table_column SET column_comment='角色JSON' WHERE table_id=23 AND column_name='roles_json';
UPDATE gen_table_column SET column_comment='头像JSON' WHERE table_id=23 AND column_name='avatars_json';
UPDATE gen_table_column SET column_comment='使用率' WHERE table_id=23 AND column_name='use_rate';
UPDATE gen_table_column SET column_comment='持有率' WHERE table_id=23 AND column_name='has_rate';

-- ads_char_summary (21)
UPDATE gen_table_column SET column_comment='角色名称' WHERE table_id=21 AND column_name='char_name';
UPDATE gen_table_column SET column_comment='版本' WHERE table_id=21 AND column_name='version_name';
UPDATE gen_table_column SET column_comment='星级' WHERE table_id=21 AND column_name='star';
UPDATE gen_table_column SET column_comment='持有数' WHERE table_id=21 AND column_name='own_count';
UPDATE gen_table_column SET column_comment='使用数' WHERE table_id=21 AND column_name='use_count';
UPDATE gen_table_column SET column_comment='持有率' WHERE table_id=21 AND column_name='own_rate';
UPDATE gen_table_column SET column_comment='使用率' WHERE table_id=21 AND column_name='use_rate';
UPDATE gen_table_column SET column_comment='平均命座' WHERE table_id=21 AND column_name='avg_constellation';
UPDATE gen_table_column SET column_comment='平均等级' WHERE table_id=21 AND column_name='avg_level';
UPDATE gen_table_column SET column_comment='排名' WHERE table_id=21 AND column_name='rank_num';
UPDATE gen_table_column SET column_comment='总用户数' WHERE table_id=21 AND column_name='total_users';
UPDATE gen_table_column SET column_comment='创建时间' WHERE table_id=21 AND column_name='created_at';

-- ads_char_momentum (22)
UPDATE gen_table_column SET column_comment='角色名称' WHERE table_id=22 AND column_name='char_name';
UPDATE gen_table_column SET column_comment='上期使用率' WHERE table_id=22 AND column_name='prev_rate';
UPDATE gen_table_column SET column_comment='当期使用率' WHERE table_id=22 AND column_name='curr_rate';
UPDATE gen_table_column SET column_comment='涨跌幅' WHERE table_id=22 AND column_name='trend';
UPDATE gen_table_column SET column_comment='头像' WHERE table_id=22 AND column_name='avatar';
UPDATE gen_table_column SET column_comment='创建时间' WHERE table_id=22 AND column_name='created_at';
