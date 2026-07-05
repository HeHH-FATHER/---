package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * 源统计 JSON 的顶层结构。
 *
 * 对应 stats.json 根对象。使用 Jackson 反序列化，
 * 未映射的字段（title, update, version_id 等）会被忽略。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatsDocument {

    /** 总样本数（参与统计的用户数） */
    public int samples;

    /** 角色总数 */
    public int char_count;

    /** 预定义配队总数（固定 100） */
    public int team_count;

    /** 梯队数量（固定 5：S+ / S / A / B / C） */
    public int tier_count;

    /** 全部角色统计数据（115 个） */
    public List<CharacterStats> chars;

    /** 全部预定义配队（100 支） */
    public List<TeamEntry> teams;

    /** 全部梯队（5 个），每个梯队内含角色命座分布 */
    public List<TierEntry> tiers;
}
