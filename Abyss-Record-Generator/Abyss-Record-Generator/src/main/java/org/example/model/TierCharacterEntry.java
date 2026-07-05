package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 源统计数据中 Tiers 板块的单角色命座分布。
 *
 * 对应 stats.json 中 "tiers" → 各 tier → "chars" 数组的每个元素。
 * c0/c2/c6_rate 之和通常 < 100%，差额为 C1/C3/C4/C5 的占比。
 *
 * 限定五星的 c0+c2+c6 通常 > 80%（集中在关键命座），
 * 常驻五星通常 < 70%（分布更分散），
 * 四星通常 > 85%（C6 占比极高）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TierCharacterEntry {

    /** 角色中文名 */
    public String name;

    /** 星级：4 或 5 */
    public int star;

    /** 0 命占比（%），即停留在 C0 的用户比例 */
    public double c0_rate;

    /** 2 命占比（%），即至少 C2 的用户比例（含 C2 以上的也计入此处口径） */
    public double c2_rate;

    /** 6 命占比（%），即满命的用户比例 */
    public double c6_rate;
}
