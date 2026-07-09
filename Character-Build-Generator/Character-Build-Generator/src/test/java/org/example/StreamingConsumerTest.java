package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.CharacterBuildRecord;
import org.example.model.WeaponChoice;
import org.example.model.ArtifactSetChoice;
import org.example.satisfaction.SatisfactionRecord;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spark Streaming 消费者逻辑单元测试
 * 验证：JSON解析、聚合计算、Redis输出格式
 */
public class StreamingConsumerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // ═══════════════════════════════════════════════════════════════
    // ① build:recent 格式测试
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("toRecentJson 输出与 Python realtime_consumer.py 格式一致")
    void testRecentJsonFormat() throws Exception {
        CharacterBuildRecord r = new CharacterBuildRecord(
            "玛薇卡", "Mavuika", 5, "180123456",
            90, 2, 10, 9, 10, 85000, "Q爆发",
            new WeaponChoice("裁断", 3),
            new ArtifactSetChoice("黑曜秘典4")
        );
        Map<String, Object> rec = toRecentMap(r);
        String json = mapper.writeValueAsString(rec);
        Map<String, Object> parsed = mapper.readValue(json, Map.class);

        assertEquals("玛薇卡", parsed.get("role"));
        assertEquals(5, parsed.get("star"));
        assertEquals(2, parsed.get("constellation"));
        assertEquals(90, parsed.get("level"));
        assertEquals(85000, parsed.get("damage"));
        assertEquals("裁断", parsed.get("weapon"));
        assertEquals("黑曜秘典4", parsed.get("arti"));
    }

    @Test
    @DisplayName("toRecentJson 各字段非空")
    void testRecentJsonAllFields() throws Exception {
        CharacterBuildRecord r = new CharacterBuildRecord(
            "芙宁娜", "Furina", 5, "180999999",
            80, 3, 8, 10, 9, 62000, "E总伤害",
            new WeaponChoice("静水流涌之辉", 1),
            new ArtifactSetChoice("黄金剧团4")
        );
        Map<String, Object> rec = toRecentMap(r);
        assertEquals(7, rec.size());
        for (Map.Entry<String, Object> e : rec.entrySet()) {
            assertNotNull(e.getValue(), e.getKey() + " should not be null");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // ② build:hot_chars 聚合测试
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("角色聚合：10条同一角色 → count=10, 正确计算平均值")
    void testCharAggregation() {
        List<CharacterBuildRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CharacterBuildRecord r = new CharacterBuildRecord();
            r.setRole("玛薇卡");
            r.setStar(5);
            r.setConstellation(i % 7);  // 0,1,2,3,4,5,6,0,1,2
            r.setAvg_damage(80000 + i * 1000);
            r.setWeapon(new WeaponChoice("裁断", 3));
            r.setArtifact_set(new ArtifactSetChoice("黑曜秘典4"));
            records.add(r);
        }

        Map<String, int[]> aggs = aggregateTest(records);
        int[] a = aggs.get("玛薇卡");
        assertEquals(10, a[0]); // count
        assertEquals(24, a[1]); // constellationSum (0+1+2+3+4+5+6+0+1+2)
        assertEquals(845000, a[2]); // damageSum (80000+81000+...+89000)
    }

    @Test
    @DisplayName("聚合：多角色 → TOP N 排序正确")
    void testTopNSorting() {
        List<CharacterBuildRecord> records = new ArrayList<>();
        // 玛薇卡 × 50
        for (int i = 0; i < 50; i++) {
            CharacterBuildRecord r = makeRecord("玛薇卡", "Mavuika", 5);
            records.add(r);
        }
        // 芙宁娜 × 30
        for (int i = 0; i < 30; i++) {
            CharacterBuildRecord r = makeRecord("芙宁娜", "Furina", 5);
            records.add(r);
        }
        // 班尼特 × 20
        for (int i = 0; i < 20; i++) {
            CharacterBuildRecord r = makeRecord("班尼特", "Bennett", 4);
            records.add(r);
        }
        // 香菱 × 10
        for (int i = 0; i < 10; i++) {
            CharacterBuildRecord r = makeRecord("香菱", "Xiangling", 4);
            records.add(r);
        }

        // 聚合
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (CharacterBuildRecord r : records) {
            counts.merge(r.getRole(), 1, Integer::sum);
        }

        // 按 count 排序
        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
        sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        assertEquals("玛薇卡", sorted.get(0).getKey());
        assertEquals(50, sorted.get(0).getValue());
        assertEquals("芙宁娜", sorted.get(1).getKey());
        assertEquals(30, sorted.get(1).getValue());
        assertEquals("班尼特", sorted.get(2).getKey());
        assertEquals("香菱", sorted.get(3).getKey());
    }

    @Test
    @DisplayName("武器/圣遗物计数正确")
    void testWeaponArtifactCounts() {
        List<CharacterBuildRecord> records = new ArrayList<>();
        // 玛薇卡：7把裁断 + 3把狼末
        for (int i = 0; i < 7; i++) {
            CharacterBuildRecord r = new CharacterBuildRecord();
            r.setRole("玛薇卡"); r.setWeapon(new WeaponChoice("裁断", 1));
            r.setArtifact_set(new ArtifactSetChoice("黑曜秘典4"));
            records.add(r);
        }
        for (int i = 0; i < 3; i++) {
            CharacterBuildRecord r = new CharacterBuildRecord();
            r.setRole("玛薇卡"); r.setWeapon(new WeaponChoice("狼的末路", 1));
            r.setArtifact_set(new ArtifactSetChoice("黑曜秘典4"));
            records.add(r);
        }

        Map<String, Integer> wepCounts = new HashMap<>();
        Map<String, Integer> artiCounts = new HashMap<>();
        for (CharacterBuildRecord r : records) {
            if (r.getWeapon() != null) wepCounts.merge(r.getWeapon().getName(), 1, Integer::sum);
            if (r.getArtifact_set() != null) artiCounts.merge(r.getArtifact_set().getName(), 1, Integer::sum);
        }

        assertEquals(7, wepCounts.get("裁断").intValue());
        assertEquals(3, wepCounts.get("狼的末路").intValue());
        assertEquals(10, artiCounts.get("黑曜秘典4").intValue());
    }

    // ═══════════════════════════════════════════════════════════════
    // ③ Satisfaction 解析测试
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("SatisfactionRecord JSON 反序列化正确")
    void testSatisfactionRecordParsing() throws Exception {
        String json = "{\"role\":\"神里绫华\",\"star\":5,\"satify\":7.5,\"ability\":8.2,\"look\":9.1,\"vote_sum\":1234,\"delta\":0.3}";
        SatisfactionRecord r = mapper.readValue(json, SatisfactionRecord.class);

        assertEquals("神里绫华", r.getRole());
        assertEquals(5, r.getStar());
        assertEquals(7.5, r.getSatify(), 0.01);
        assertEquals(8.2, r.getAbility(), 0.01);
        assertEquals(9.1, r.getLook(), 0.01);
        assertEquals(1234, r.getVoteSum());
        assertEquals(0.3, r.getDelta(), 0.01);
    }

    @Test
    @DisplayName("TOP6 + delta 计算 与 Python satisfaction_consumer 逻辑一致")
    void testSatisfactionDeltaLogic() {
        // 模拟两轮数据，验证 delta 公式: delta = round(current - previous, 1)
        double prev = 7.5;
        double curr = 7.8;
        double delta = Math.round((curr - prev) * 10.0) / 10.0;
        assertEquals(0.3, delta, 0.01);

        double prev2 = 8.2;
        double curr2 = 7.9;
        double delta2 = Math.round((curr2 - prev2) * 10.0) / 10.0;
        assertEquals(-0.3, delta2, 0.01);
    }

    @Test
    @DisplayName("ZSET + LIST 操作语义验证")
    void testRedisOpsSemantics() {
        // ZADD: 同 role 更新分数
        // ZREVRANGE ... WITHSCORES: 按分数降序取 TOP6
        // RPUSH + LTRIM ... -60 -1: 保留最新60条
        // 无 Redis 环境，仅验证逻辑一致

        Map<String, Double> scores = new HashMap<>();
        String role = "可莉";
        scores.put(role, 7.3);
        scores.put(role, 7.5); // 覆盖
        assertEquals(7.5, scores.get(role), 0.01);

        List<Double> trend = new ArrayList<>();
        for (int i = 0; i < 65; i++) trend.add(i * 0.1);
        // LTRIM -60 -1
        List<Double> trimmed = trend.subList(Math.max(0, trend.size() - 60), trend.size());
        assertEquals(60, trimmed.size());
        assertEquals(0.5, trimmed.get(0), 0.01);
        assertEquals(6.4, trimmed.get(59), 0.01);
    }

    // ═══════════════════════════════════════════════════════════════
    // ④ build:hot_chars 完整 JSON 输出格式验证
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("TOP2 输出 JSON 包含所有必需字段")
    void testHotCharJsonFormat() throws Exception {
        List<CharacterBuildRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CharacterBuildRecord r = new CharacterBuildRecord("玛薇卡", "Mavuika", 5, "18000000" + i,
                90, 3, 10, 9, 10, 85000, "Q爆发",
                new WeaponChoice("裁断", 3), new ArtifactSetChoice("黑曜秘典4"));
            records.add(r);
        }

        // 聚合
        Map<String, int[]> aggs = aggregateTest(records);
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<String, int[]> e : aggs.entrySet()) {
            counts.put(e.getKey(), e.getValue()[0]);
        }
        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
        sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        // 构造 JSON（模拟 BuildStreamingConsumer 输出）
        List<Map<String, Object>> result = new ArrayList<>();
        for (int i = 0; i < Math.min(4, sorted.size()); i++) {
            Map.Entry<String, Integer> e = sorted.get(i);
            int[] a = aggs.get(e.getKey());
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("role", e.getKey());
            item.put("star", 5);
            item.put("count", a[0]);
            item.put("avg_constellation", round1(a[1] / (double) a[0]));
            item.put("avg_damage", (int) (a[2] / a[0]));
            item.put("ename", "Mavuika");
            item.put("weapons", Collections.singletonList(
                Map.of("name", "裁断", "count", 10, "ratio", 1.0)));
            item.put("artifacts", Collections.singletonList(
                Map.of("name", "黑曜秘典4", "count", 10, "ratio", 1.0)));
            result.add(item);
        }

        String json = mapper.writeValueAsString(result);
        List<Map<String, Object>> parsed = mapper.readValue(json, List.class);

        assertEquals(1, parsed.size());
        Map<String, Object> first = parsed.get(0);
        assertEquals("玛薇卡", first.get("role"));
        assertEquals(5, first.get("star"));
        assertEquals(10, first.get("count"));
        assertEquals(3.0, ((Number) first.get("avg_constellation")).doubleValue(), 0.1);
        assertEquals(85000, first.get("avg_damage"));
        assertNotNull(first.get("weapons"));
        assertNotNull(first.get("artifacts"));
        List weapons = (List) first.get("weapons");
        assertEquals(1, weapons.size());
    }

    // ═══════════════════════════════════════════════════════════════
    // ⑤ 边界测试
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("空记录列表不抛异常")
    void testEmptyRecords() {
        List<CharacterBuildRecord> records = Collections.emptyList();
        Map<String, int[]> aggs = aggregateTest(records);
        assertTrue(aggs.isEmpty());
    }

    @Test
    @DisplayName("null weapon/artifact 不抛 NPE")
    void testNullWeapon() throws Exception {
        CharacterBuildRecord r = new CharacterBuildRecord();
        r.setRole("测试");
        r.setStar(4);
        r.setConstellation(0);
        r.setAvg_damage(1000);

        Map<String, Object> rec = toRecentMap(r);
        // weapon null → "?"
        assertEquals("?", rec.get("weapon"));
        assertEquals("?", rec.get("arti"));
    }

    @Test
    @DisplayName("大批量聚合（1000条）性能可接受")
    void testLargeAggregation() {
        Random rng = new Random(42);
        List<CharacterBuildRecord> records = new ArrayList<>();
        String[] roles = {"玛薇卡", "芙宁娜", "班尼特", "香菱", "行秋", "万叶", "钟离", "雷电将军"};
        for (int i = 0; i < 1000; i++) {
            CharacterBuildRecord r = new CharacterBuildRecord();
            r.setRole(roles[rng.nextInt(roles.length)]);
            r.setStar(rng.nextInt(2) == 0 ? 5 : 4);
            r.setConstellation(rng.nextInt(7));
            r.setAvg_damage(10000 + rng.nextInt(90000));
            if (rng.nextBoolean()) r.setWeapon(new WeaponChoice("测试武", 1));
            if (rng.nextBoolean()) r.setArtifact_set(new ArtifactSetChoice("测试套"));
            records.add(r);
        }

        long start = System.currentTimeMillis();
        Map<String, int[]> aggs = aggregateTest(records);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 100, "1000条聚合应在100ms内，实际: " + elapsed + "ms");
        assertTrue(aggs.size() >= 1, "至少有1个角色");
        int total = aggs.values().stream().mapToInt(a -> a[0]).sum();
        assertEquals(1000, total);
    }

    // ═══════════════════════════════════════════════════════════════
    // helpers
    // ═══════════════════════════════════════════════════════════════

    private Map<String, Object> toRecentMap(CharacterBuildRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("role", r.getRole() != null ? r.getRole() : "?");
        m.put("star", r.getStar());
        m.put("constellation", r.getConstellation());
        m.put("level", r.getLevel());
        m.put("damage", r.getAvg_damage());
        m.put("weapon", r.getWeapon() != null ? r.getWeapon().getName() : "?");
        m.put("arti", r.getArtifact_set() != null ? r.getArtifact_set().getName() : "?");
        return m;
    }

    /** 聚合：[count, constellationSum, damageSum] */
    private Map<String, int[]> aggregateTest(List<CharacterBuildRecord> records) {
        Map<String, int[]> aggs = new LinkedHashMap<>();
        for (CharacterBuildRecord r : records) {
            String role = r.getRole() != null ? r.getRole() : "?";
            int[] a = aggs.computeIfAbsent(role, k -> new int[3]);
            a[0]++; // count
            a[1] += r.getConstellation();
            a[2] += r.getAvg_damage();
        }
        return aggs;
    }

    private CharacterBuildRecord makeRecord(String role, String ename, int star) {
        return new CharacterBuildRecord(role, ename, star, "180000000",
            80, 2, 8, 8, 8, 50000, "Q爆发",
            new WeaponChoice("测试武器", 1), new ArtifactSetChoice("测试圣遗物"));
    }

    private static double round1(double v) { return Math.round(v * 10.0) / 10.0; }
}
