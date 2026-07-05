package org.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * 源统计数据中 Teams 板块的一个配队。
 *
 * 对应 stats.json 中 "teams" 数组的每个元素。
 * 共 100 支预定义队伍，用户只能从中选择两支（且两队角色不能重叠）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TeamEntry {

    /** 队伍成员列表（1~4 人，绝大多数为 4 人，仅极少数为 1 人单通） */
    public List<TeamMember> members;

    /** 该队伍的使用率（%），即使用此队伍的用户数 / 总样本数 * 100 */
    public double use_rate;

    /** 持有率（%），即拥有该队伍全部成员的用户占比 */
    public double has_rate;
}
