package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 队伍中的单个成员。
 *
 * 对应 stats.json 中 "teams" → members 数组的每个元素。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TeamMember {

    /** 星级：4 或 5 */
    public int star;

    /** 角色中文名 */
    public String name;

    /** 角色头像图床链接（稳定不变，可作为角色唯一标识的 hash） */
    public String avatar;
}
