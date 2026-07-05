package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 源统计数据中单个角色的字段。
 *
 * 对应 stats.json 中 "chars" 数组的每个元素。
 * 所有字段直接映射 JSON 中的同名 key。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CharacterStats {

    /** 角色中文名，如 "玛薇卡"、"班尼特" */
    public String name;

    /** 星级：4 或 5 */
    public int star;

    /** 使用该角色通关深渊的用户数 */
    public int use_count;

    /** 拥有该角色的用户数 */
    public int own_count;

    /** 使用率（%）= use_count / 总样本数 * 100 */
    public double use_rate;

    /** 拥有率（%）= own_count / 总样本数 * 100 */
    public double own_rate;

    /** 全服平均命座数（0~6 之间的浮点数），如 5.6 表示人均约 C5~C6 */
    public double constellation;
}
