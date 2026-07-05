package org.example.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.*;
import org.example.util.SeededRandom;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用户数据生成器 —— 核心引擎。
 *
 * <p>根据源统计数据（StatsDocument）为虚拟用户生成角色BOX和深渊战绩。
 * 设计目标：大量生成后聚合，结果应近似于源统计分布。</p>
 *
 * <h3>核心流程（每个用户）</h3>
 * <ol>
 *   <li><b>自然抽取拥有角色</b>：以 own_rate 为概率做伯努利试验</li>
 *   <li><b>筛选可行配队对</b>：从预计算的 3126 对中筛出该用户角色全齐的</li>
 *   <li><b>兜底补角色</b>：极少数情况下若无一可行，从最热门配队补缺失角色</li>
 *   <li><b>加权选配队</b>：按 use_rate 乘积加权随机选一对</li>
 *   <li><b>分配命座</b>：基于 tier 数据的 c0/c2/c6 占比，区分限定五星/常驻五星/四星</li>
 * </ol>
 *
 * <h3>命座分配策略</h3>
 * 不使用简单的"均值四舍五入"，而是基于实际玩家的命座聚集效应：
 * <ul>
 *   <li><b>限定五星</b>（c0+c2+c6 ≥ 75%）：命座集中在 C0/C2/C6，
 *       极少 C1/C3/C4/C5。剩余概率中 C1 占 60%</li>
 *   <li><b>常驻五星</b>（c0+c2+c6 < 75%）：命座分布较散，
 *       因歪常驻累积，C1~C5 均有可观占比</li>
 *   <li><b>四星</b>：C6 占比极高（72~87%），剩余集中于 C3~C5</li>
 * </ul>
 */
public class UserDataGenerator {

    // ═══════════════════════════════════════
    // 成员变量
    // ═══════════════════════════════════════

    /** 源统计数据（全量） */
    private final StatsDocument stats;

    /** 主随机数生成器（用于预计算，每个用户会派生独立 RNG） */
    private final SeededRandom random;

    /** 脏数据比例 (0.0 ~ 1.0)，默认 0 即不产脏数据 */
    private final double dirtyRate;

    /** 脏数据计数器 */
    private int dirtyCount = 0;

    /** JSON 序列化器（Jackson） */
    private final ObjectMapper mapper;

    /** 角色名 → 角色统计（O(1) 查询） */
    private final Map<String, CharacterStats> charByName = new LinkedHashMap<>();

    /** 角色名 → 命座 tier 数据（O(1) 查询） */
    private final Map<String, TierCharacterEntry> tierByName = new HashMap<>();

    /** 角色列表（保持 JSON 原始顺序） */
    private final List<CharacterStats> charList;

    /** 预计算的所有可行配队对（两队角色无重叠） */
    private final List<TeamPair> teamPairs = new ArrayList<>();

    // ═══════════════════════════════════════
    // 构造
    // ═══════════════════════════════════════

    /**
     * @param stats  已加载的源统计数据
     * @param random 主随机数生成器（seed 用于整个生成会话）
     */
    public UserDataGenerator(StatsDocument stats, SeededRandom random) {
        this(stats, random, 0.0);
    }

    /**
     * @param stats     已加载的源统计数据
     * @param random    主随机数生成器（seed 用于整个生成会话）
     * @param dirtyRate 脏数据比例 (0.0 ~ 1.0)，默认 0
     */
    public UserDataGenerator(StatsDocument stats, SeededRandom random, double dirtyRate) {
        this.stats = stats;
        this.random = random;
        this.dirtyRate = Math.max(0.0, Math.min(1.0, dirtyRate));
        this.mapper = new ObjectMapper();
        this.charList = stats.chars;

        // 构建角色名索引（O(1) 查找）
        for (CharacterStats c : stats.chars) {
            charByName.put(c.name, c);
        }

        // 构建命座 tier 索引
        for (TierEntry tier : stats.tiers) {
            for (TierCharacterEntry tc : tier.chars) {
                tierByName.put(tc.name, tc);
            }
        }

        // 预计算所有合法配队对（一次性完成）
        precomputeTeamPairs();
    }

    // ═══════════════════════════════════════
    // 配队对预计算
    // ═══════════════════════════════════════

    /**
     * 一支配队对：两支队伍的组合，角色无重叠。
     * weight = teamA.use_rate × teamB.use_rate，用于加权随机选取。
     */
    private static class TeamPair {
        final int teamA;     // 队伍 A 在 stats.teams 中的下标
        final int teamB;     // 队伍 B 在 stats.teams 中的下标
        final double weight; // 选取权重（使用率乘积）

        TeamPair(int a, int b, double w) {
            this.teamA = a;
            this.teamB = b;
            this.weight = w;
        }
    }

    /**
     * 预计算所有合法配队对。
     * 从 100 支队伍中任选两支（i < j），要求两队成员完全不重叠。
     * 共 100×99/2 = 4950 种组合，其中约 3126 对合法。
     */
    private void precomputeTeamPairs() {
        List<TeamEntry> teams = stats.teams;
        int n = teams.size();

        for (int i = 0; i < n; i++) {
            Set<String> namesI = memberNames(teams.get(i));
            for (int j = i + 1; j < n; j++) {
                Set<String> namesJ = memberNames(teams.get(j));

                // 两队角色集合必须无交集（深渊上下半不能出同一角色）
                if (Collections.disjoint(namesI, namesJ)) {
                    // 权重 = 队伍 i 使用率 × 队伍 j 使用率
                    double weight = teams.get(i).use_rate * teams.get(j).use_rate;
                    teamPairs.add(new TeamPair(i, j, weight));
                }
            }
        }
    }

    /** 提取队伍中所有角色名 */
    private static Set<String> memberNames(TeamEntry team) {
        return team.members.stream().map(m -> m.name).collect(Collectors.toSet());
    }

    // ═══════════════════════════════════════
    // 主入口：生成单用户数据
    // ═══════════════════════════════════════

    /**
     * 为单个用户生成完整的角色BOX和深渊战绩。
     *
     * <p>流程：先自然决定拥有 → 筛可行配队 → (兜底补角色) → 加权选配队。
     * 绝大多数用户无需兜底（验证中 200 用户兜底次数为 0）。</p>
     *
     * @param uid     9 位用户 ID
     * @param userIdx 用户序号（用于派生独立随机序列）
     * @return 包含 boxRoot 和 recordRoot 的 UserData
     */
    public UserData generate(String uid, int userIdx) {
        // 每个用户拥有独立的随机序列：
        //   userRand = 主 seed + userIdx × 999983 (大质数)
        // 这样同 seed 不同 userIdx 的用户相互独立，且完全可复现
        SeededRandom userRand = new SeededRandom(random.getSeed() + userIdx * 999983L);

        // 第一步：按 own_rate 概率自然决定角色拥有（不做任何 forced own）
        Map<String, OwnedChar> ownedChars = decideOwnership(userRand);

        // 第二步：从预计算的 3126 对中筛出该用户可行配队对
        List<TeamPair> validPairs = findValidPairs(ownedChars);

        // 第三步：极少数用户可能无可行配队 → 兜底从最热门配队补缺失角色
        if (validPairs.isEmpty()) {
            // teamPairs 按预计算顺序排列，第 0 个权重最高（最热门的队伍组合）
            TeamPair fallback = teamPairs.get(0);
            ownedChars = forceOwnTeamMembers(userRand, ownedChars, fallback);
            validPairs = findValidPairs(ownedChars);
        }

        // 第四步：从可行配队对中按权重随机选取一对
        TeamPair pair = pickWeightedPair(userRand, validPairs);

        // 第五步：组装输出结构
        UserData data = buildUserData(uid, ownedChars, pair);

        // 第六步：按概率注入脏数据
        if (dirtyRate > 0 && userRand.nextDouble() < dirtyRate) {
            applyRandomCorruption(uid, data, userRand);
            dirtyCount++;
        }

        return data;
    }

    // ═══════════════════════════════════════
    // 第一步：自然拥有判定
    // ═══════════════════════════════════════

    /** 用户拥有的单个角色 */
    private static class OwnedChar {
        final String name;         // 角色名
        final int star;            // 星级
        final int constellation;   // 命座 (0~6)
        final int level;           // 等级 (5星=90, 4星=80)

        OwnedChar(String name, int star, int constellation) {
            this.name = name;
            this.star = star;
            this.constellation = constellation;
            this.level = (star == 5) ? 90 : 80;
        }
    }

    /**
     * 按 own_rate 做伯努利试验，决定每个角色是否拥有。
     *
     * <p>例如 own_rate=67.8 的角色，每个用户有 67.8% 概率拥有。
     * 大量用户聚合后，拥有率收敛于 own_rate。</p>
     */
    private Map<String, OwnedChar> decideOwnership(SeededRandom rand) {
        Map<String, OwnedChar> owned = new LinkedHashMap<>();

        for (CharacterStats c : charList) {
            // rand.nextDouble() * 100 ∈ [0, 100)，若 < own_rate 则拥有
            if (rand.nextDouble() * 100.0 < c.own_rate) {
                int cons = assignConstellation(c, rand);
                owned.put(c.name, new OwnedChar(c.name, c.star, cons));
            }
        }
        return owned;
    }

    // ═══════════════════════════════════════
    // 第二步：筛选可行配队对
    // ═══════════════════════════════════════

    /** 筛选该用户拥有的角色能组成的全部配队对 */
    private List<TeamPair> findValidPairs(Map<String, OwnedChar> owned) {
        List<TeamPair> valid = new ArrayList<>();
        for (TeamPair pair : teamPairs) {
            if (allMembersOwned(pair.teamA, owned)
                    && allMembersOwned(pair.teamB, owned)) {
                valid.add(pair);
            }
        }
        return valid;
    }

    /** 检查队伍中所有角色是否都被该用户拥有 */
    private boolean allMembersOwned(int teamIdx, Map<String, OwnedChar> owned) {
        for (TeamMember m : stats.teams.get(teamIdx).members) {
            if (!owned.containsKey(m.name)) {
                return false;
            }
        }
        return true;
    }

    // ═══════════════════════════════════════
    // 第三步：兜底补齐（极少触发）
    // ═══════════════════════════════════════

    /**
     * 强制将配队中的缺失角色加入用户BOX。
     *
     * <p>仅在用户自然拥有无法组成任何配队对时才调用。
     * 补齐的角色同样按 tier 数据分配合理命座。</p>
     */
    private Map<String, OwnedChar> forceOwnTeamMembers(SeededRandom rand,
                                                        Map<String, OwnedChar> owned,
                                                        TeamPair pair) {
        // 复制原有拥有表（不修改原对象）
        Map<String, OwnedChar> result = new LinkedHashMap<>(owned);

        // 收集两队所有角色名
        Set<String> needed = new HashSet<>();
        for (TeamMember m : stats.teams.get(pair.teamA).members) needed.add(m.name);
        for (TeamMember m : stats.teams.get(pair.teamB).members) needed.add(m.name);

        // 补齐缺失角色
        for (String name : needed) {
            if (!result.containsKey(name)) {
                CharacterStats cs = charByName.get(name);
                if (cs != null) {
                    int cons = assignConstellation(cs, rand);
                    result.put(name, new OwnedChar(cs.name, cs.star, cons));
                }
            }
        }
        return result;
    }

    // ═══════════════════════════════════════
    // 第四步：加权随机选配队对
    // ═══════════════════════════════════════

    /**
     * 按权重从可行配队对中随机选取一对。
     *
     * <p>权重 = teamA.use_rate × teamB.use_rate，
     * 因此源数据中使用率越高的队伍组合被选中的概率越大。</p>
     */
    private TeamPair pickWeightedPair(SeededRandom rand, List<TeamPair> validPairs) {
        // 计算总权重
        double totalW = 0;
        for (TeamPair p : validPairs) totalW += p.weight;

        // 轮盘赌选择：生成 [0, totalW) 随机数，看落在哪个区间
        double roll = rand.nextDouble() * totalW;
        double cum = 0;
        for (TeamPair p : validPairs) {
            cum += p.weight;
            if (roll < cum) {
                return p;
            }
        }
        // 浮点精度兜底：返回最后一个
        return validPairs.get(validPairs.size() - 1);
    }

    // ═══════════════════════════════════════
    // 命座分配
    // ═══════════════════════════════════════

    /**
     * 为指定角色分配命座（0~6）。
     *
     * <p>不使用简单的"平均命座四舍五入"，而是基于 tier 数据中
     * c0/c2/c6 的实际占比模拟玩家命座聚集效应。</p>
     *
     * <p>分配区间：
     * <ul>
     *   <li>p0 = c0_rate / 100 → 分配到 C0</li>
     *   <li>p2 = c2_rate / 100 → 分配到 C2</li>
     *   <li>p6 = c6_rate / 100 → 分配到 C6</li>
     *   <li>剩余 = 1 - p0 - p2 - p6 → 分配到 C1/C3/C4/C5</li>
     * </ul></p>
     */
    private int assignConstellation(CharacterStats c, SeededRandom rand) {
        TierCharacterEntry tier = tierByName.get(c.name);

        // 无 tier 数据时退化：均值四舍五入
        if (tier == null) {
            return clampConstellation((int) Math.round(c.constellation));
        }

        double c0 = tier.c0_rate;
        double c2 = tier.c2_rate;
        double c6 = tier.c6_rate;
        double sum = c0 + c2 + c6;

        if (sum <= 0.0) {
            return 0;  // 无数据，默认 C0
        }

        // 归一化：将百分比转为概率
        double p0 = c0 / 100.0;
        double p2 = c2 / 100.0;
        double p6 = c6 / 100.0;

        double roll = rand.nextDouble();

        // 轮盘赌：看随机数落在哪个区间
        if (roll < p0) {
            return 0;   // C0 — 零命
        } else if (roll < p0 + p2) {
            return 2;   // C2 — 关键命座
        } else if (roll < p0 + p2 + p6) {
            return 6;   // C6 — 满命
        } else {
            // 剩余区间 → C1, C3, C4, C5
            return pickRemainingConstellation(c.star, tier, rand);
        }
    }

    /**
     * 从 {C1, C3, C4, C5} 中按角色类型加权选取。
     *
     * <p>数组索引对应关系：weights[0]=C1, weights[1]=C2,
     * weights[2]=C3, weights[3]=C4, weights[4]=C5, weights[5]=C6</p>
     *
     * @param star 角色星级（用于判断四星/五星）
     * @param tier 该角色的 tier 数据（用于判断限定/常驻五星）
     */
    private int pickRemainingConstellation(int star, TierCharacterEntry tier,
                                           SeededRandom rand) {
        double[] weights;

        if (star == 4) {
            // ── 四星角色 ──
            // 大多数玩家四星已满命或接近满命
            // 剩余区间集中于 C3(30%) / C4(30%) / C5(40%)
            weights = new double[]{0.0, 0.30, 0.0, 0.30, 0.40, 0.0};
        } else {
            // ── 五星角色 ──
            double tierSum = tier.c0_rate + tier.c2_rate + tier.c6_rate;

            if (tierSum >= 75.0) {
                // 限定五星：命座高度集中于 C0/C2/C6
                // 中位数玩家抽 0 命，进阶玩家抽 2 命，氪佬抽 6 命
                // 很少有人停在 C1/C3/C4/C5
                // 剩余中 C1 占大头（部分玩家会补个一命）
                weights = new double[]{0.60, 0.0, 0.15, 0.10, 0.15, 0.0};
            } else {
                // 常驻五星：命座分布较散（歪出来的）
                // 累积多次 50/50 失败 → C1~C6 自然分布
                weights = new double[]{0.35, 0.0, 0.25, 0.20, 0.20, 0.0};
            }
        }

        // ── 归一化权重并轮盘赌 ──
        double totalW = 0;
        for (double w : weights) totalW += w;

        double roll = rand.nextDouble() * totalW;
        double cumulative = 0;
        for (int i = 0; i < weights.length; i++) {
            cumulative += weights[i];
            if (roll < cumulative) {
                return i + 1;  // weights[0]→C1, weights[1]→C2, ...
            }
        }
        return 1;  // 兜底 C1
    }

    /** 将命座值钳制在 [0, 6] 区间 */
    private static int clampConstellation(int c) {
        return Math.max(0, Math.min(6, c));
    }

    // ═══════════════════════════════════════
    // 输出构建
    // ═══════════════════════════════════════

    /**
     * 组装单个用户的输出数据结构（角色BOX + 深渊战绩）。
     *
     * @return UserData，内含 boxRoot 和 recordRoot 两个 Map，
     *         可直接序列化为 JSON
     */
    private UserData buildUserData(String uid, Map<String, OwnedChar> owned,
                                   TeamPair pair) {
        // ── 角色BOX ──
        List<Map<String, Object>> charBox = new ArrayList<>();
        for (OwnedChar oc : owned.values()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", oc.name);
            entry.put("star", oc.star);
            entry.put("constellation", oc.constellation);
            entry.put("level", oc.level);
            charBox.add(entry);
        }

        Map<String, Object> boxRoot = new LinkedHashMap<>();
        boxRoot.put("uid", uid);
        boxRoot.put("characters", charBox);

        // ── 深渊战绩 ──
        List<Map<String, Object>> teamList = new ArrayList<>();
        teamList.add(buildTeamEntry(1, pair.teamA));  // 上半
        teamList.add(buildTeamEntry(2, pair.teamB));  // 下半

        Map<String, Object> recordRoot = new LinkedHashMap<>();
        recordRoot.put("uid", uid);
        recordRoot.put("teams", teamList);

        return new UserData(uid, boxRoot, recordRoot);
    }

    /**
     * 构建单个队伍条目的输出结构。
     *
     * @param half    半场编号（1=上半, 2=下半）
     * @param teamIdx 队伍在 stats.teams 中的下标
     */
    private Map<String, Object> buildTeamEntry(int half, int teamIdx) {
        TeamEntry team = stats.teams.get(teamIdx);
        List<Map<String, Object>> members = new ArrayList<>();
        for (TeamMember m : team.members) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", m.name);
            entry.put("star", m.star);
            members.add(entry);
        }

        Map<String, Object> teamEntry = new LinkedHashMap<>();
        teamEntry.put("half", half);
        teamEntry.put("team_index", teamIdx + 1);  // 输出用 1-based
        teamEntry.put("members", members);
        return teamEntry;
    }

    // ═══════════════════════════════════════
    // 文件写入
    // ═══════════════════════════════════════

    /**
     * 将单个用户数据写入磁盘。
     *
     * <p>每个用户生成两个文件：
     * <ul>
     *   <li>{uid}_角色box.json — 角色BOX</li>
     *   <li>{uid}_深渊战绩.json — 深渊战绩</li>
     * </ul></p>
     */
    public void writeUserFiles(String uid, UserData data, File outDir) throws IOException {
        File boxFile = new File(outDir, uid + "_char_box.json");
        File recordFile = new File(outDir, uid + "_abyss_record.json");

        // 使用 PrettyPrinter 输出格式化 JSON（方便人工查看）
        mapper.writerWithDefaultPrettyPrinter().writeValue(boxFile, data.boxRoot);
        mapper.writerWithDefaultPrettyPrinter().writeValue(recordFile, data.recordRoot);
    }

    // ═══════════════════════════════════════
    // Getter
    // ═══════════════════════════════════════

    /** @return 预计算的合法配队对总数（通常 ≈3126） */
    public int getTeamPairCount() {
        return teamPairs.size();
    }

    /** @return 脏数据用户计数 */
    public int getDirtyCount() {
        return dirtyCount;
    }

    // ═══════════════════════════════════════
    // 脏数据注入
    // ═══════════════════════════════════════

    @SuppressWarnings("unchecked")
    private void applyRandomCorruption(String uid, UserData data, SeededRandom rand) {
        double roll = rand.nextDouble();

        if (roll < 0.20) {
            // bad_star: 随机改一个角色 star = 0 / 3 / 99
            corruptStar(data, rand);
        } else if (roll < 0.40) {
            // bad_const: 随机改一个角色 constellation = -1 / 7 / 99
            corruptConstellation(data, rand);
        } else if (roll < 0.55) {
            // bad_level: 随机改一个角色 level = 0 / 100
            corruptLevel(data, rand);
        } else if (roll < 0.70) {
            // missing_field: 随机清空一个角色 name = ""
            corruptMissingField(data);
        } else if (roll < 0.80) {
            // overlap: 把上半一个角色复制到下半
            corruptOverlap(data);
        } else if (roll < 0.90) {
            // missing_char: 在战绩中插入一个不在 BOX 中的随机角色
            corruptMissingChar(data, rand);
        } else if (roll < 0.95) {
            // bad_team_size: members 塞成 0 个或 5 个以上
            corruptTeamSize(data, rand);
        } else if (roll < 0.98) {
            // bad_uid: uid 改成另一个随机值
            corruptUid(uid, data, rand);
        } else {
            // empty_box: characters 数组清空
            corruptEmptyBox(data);
        }
    }

    private void corruptStar(UserData data, SeededRandom rand) {
        List<Map<String, Object>> chars = getChars(data);
        if (chars.isEmpty()) return;
        Map<String, Object> c = chars.get(rand.nextInt(chars.size()));
        int[] badValues = {0, 3, 99};
        c.put("star", badValues[rand.nextInt(badValues.length)]);
    }

    private void corruptConstellation(UserData data, SeededRandom rand) {
        List<Map<String, Object>> chars = getChars(data);
        if (chars.isEmpty()) return;
        Map<String, Object> c = chars.get(rand.nextInt(chars.size()));
        int[] badValues = {-1, 7, 99};
        c.put("constellation", badValues[rand.nextInt(badValues.length)]);
    }

    private void corruptLevel(UserData data, SeededRandom rand) {
        List<Map<String, Object>> chars = getChars(data);
        if (chars.isEmpty()) return;
        Map<String, Object> c = chars.get(rand.nextInt(chars.size()));
        int[] badValues = {0, 100};
        c.put("level", badValues[rand.nextInt(badValues.length)]);
    }

    private void corruptMissingField(UserData data) {
        List<Map<String, Object>> chars = getChars(data);
        if (chars.isEmpty()) return;
        chars.get(0).put("name", "");
    }

    @SuppressWarnings("unchecked")
    private void corruptOverlap(UserData data) {
        List<Map<String, Object>> teams = (List<Map<String, Object>>) data.recordRoot.get("teams");
        if (teams == null || teams.size() < 2) return;
        List<Map<String, Object>> members1 = (List<Map<String, Object>>) teams.get(0).get("members");
        List<Map<String, Object>> members2 = (List<Map<String, Object>>) teams.get(1).get("members");
        if (members1 == null || members1.isEmpty() || members2 == null) return;
        // 复制上半第一个角色到下半
        Map<String, Object> copy = new LinkedHashMap<>(members1.get(0));
        members2.add(copy);
    }

    @SuppressWarnings("unchecked")
    private void corruptMissingChar(UserData data, SeededRandom rand) {
        List<Map<String, Object>> teams = (List<Map<String, Object>>) data.recordRoot.get("teams");
        if (teams == null || teams.isEmpty()) return;
        List<Map<String, Object>> members = (List<Map<String, Object>>) teams.get(0).get("members");
        if (members == null) return;
        Map<String, Object> fake = new LinkedHashMap<>();
        fake.put("name", "？？？不存在的角色" + rand.nextInt(1000));
        fake.put("star", 5);
        members.add(fake);
    }

    @SuppressWarnings("unchecked")
    private void corruptTeamSize(UserData data, SeededRandom rand) {
        List<Map<String, Object>> teams = (List<Map<String, Object>>) data.recordRoot.get("teams");
        if (teams == null || teams.isEmpty()) return;
        List<Map<String, Object>> members = (List<Map<String, Object>>) teams.get(0).get("members");
        if (members == null) return;
        if ((rand.nextDouble() < 0.5)) {
            members.clear(); // 0 人
        } else {
            for (int i = 0; i < 5; i++) { // 加到 5 人以上
                Map<String, Object> extra = new LinkedHashMap<>();
                extra.put("name", "幽灵角色" + i);
                extra.put("star", 5);
                members.add(extra);
            }
        }
    }

    private void corruptUid(String uid, UserData data, SeededRandom rand) {
        // 战绩 uid 改成另一个随机值，BOX uid 清空
        int fakeUid = 100000000 + rand.nextInt(99999999);
        if ((rand.nextDouble() < 0.5)) {
            data.recordRoot.put("uid", String.valueOf(fakeUid)); // BOX和战绩uid不一致
        } else {
            data.boxRoot.put("uid", "");  // uid为空
            data.recordRoot.put("uid", "");
        }
    }

    @SuppressWarnings("unchecked")
    private void corruptEmptyBox(UserData data) {
        ((List<Map<String, Object>>) data.boxRoot.get("characters")).clear();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getChars(UserData data) {
        return (List<Map<String, Object>>) data.boxRoot.get("characters");
    }

    /** @return 源统计数据（只读） */
    public StatsDocument getStats() {
        return stats;
    }

    // ═══════════════════════════════════════
    // 内部类：单用户生成结果
    // ═══════════════════════════════════════

    /**
     * 单个用户的生成结果容器。
     *
     * <p>包含两个待序列化的 Map：
     * <ul>
     *   <li>boxRoot → 角色BOX JSON</li>
     *   <li>recordRoot → 深渊战绩 JSON</li>
     * </ul></p>
     */
    public static class UserData {
        public final String uid;
        public final Map<String, Object> boxRoot;
        public final Map<String, Object> recordRoot;

        UserData(String uid, Map<String, Object> boxRoot, Map<String, Object> recordRoot) {
            this.uid = uid;
            this.boxRoot = boxRoot;
            this.recordRoot = recordRoot;
        }
    }
}
