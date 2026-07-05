package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * 源统计数据中 Tiers 板块的一个梯队。
 *
 * 对应 stats.json 中 "tiers" 数组的每个元素。
 * 梯队按使用率从高到低排列：S+ → S → A → B → C。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TierEntry {

    /** 梯队名称，如 "S+"、"S"、"A"、"B"、"C" */
    public String rank_name;

    /** 该梯队包含的角色列表，每个角色带有 c0/c2/c6 命座占比 */
    public List<TierCharacterEntry> chars;
}
