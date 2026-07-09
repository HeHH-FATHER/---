package org.example.satisfaction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 满意度实时生成器 — 模拟玩家投票波动
 * 与 Python satisfaction_producer.py 逻辑一致：
 * 每 tick 随机选部分角色，±0.3 波动（30%概率大幅±1.0）
 */
public class SatisfactionGenerator {

    private final Random rng;
    private final Map<String, CharState> chars = new LinkedHashMap<>();
    private final Map<String, Double> prevScores = new HashMap<>();

    private static class CharState {
        int star;
        double satifySum;
        double abilitySum;
        double lookSum;
        long voteSum;
        double origSatify;  // 原始分数（均值回归锚点）
        CharState(int star, double s, double a, double l, long v) {
            this.star = star; satifySum = s; abilitySum = a; lookSum = l; voteSum = v;
            this.origSatify = s / v;
        }
    }

    public SatisfactionGenerator(String dataPath) throws IOException {
        this(new File(dataPath));
    }

    public SatisfactionGenerator(File dataFile) throws IOException {
        rng = new Random();
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> raw = mapper.readValue(dataFile,
                new TypeReference<List<Map<String, Object>>>() {});

        for (Map<String, Object> d : raw) {
            String role = (String) d.get("role");
            int star = (int) d.getOrDefault("star", 4);
            long v = Math.max(((Number) d.getOrDefault("vote_sum", 1)).longValue(), 1);
            double s = toDouble(d, "avg_satify", 5) * v;
            double a = toDouble(d, "avg_ability", 5) * v;
            double l = toDouble(d, "avg_look", 5) * v;
            chars.put(role, new CharState(star, s, a, l, v));
        }
    }

    private double toDouble(Map<String, Object> m, String key, double def) {
        Object v = m.get(key);
        if (v instanceof Number) return ((Number) v).doubleValue();
        return def;
    }

    /**
     * 模拟一轮舆论波动，返回 TOP6 + 趋势全量 JSON
     */
    public List<SatisfactionRecord> tick() {
        List<String> names = new ArrayList<>(chars.keySet());
        Collections.shuffle(names, rng);

        int n = 5 + rng.nextInt(36); // 5~40 角色
        for (int i = 0; i < n && i < names.size(); i++) {
            CharState c = chars.get(names.get(i));
            double avg = c.satifySum / c.voteSum;
            // 随机游走 + 边界斥力：越近10越易降，越近1越易升
            double bias = (5.0 - avg) * 0.05; // 向中心5分微偏
            double boundary;
            if (avg > 9.0) boundary = -rng.nextDouble() * 0.8;      // >9分强制下拉
            else if (avg < 2.0) boundary = rng.nextDouble() * 0.8;  // <2分强制上拉
            else boundary = 0;
            double delta;
            if (rng.nextDouble() < 0.3) {
                delta = -1.0 + rng.nextDouble() * 2.0 + bias + boundary;
            } else {
                delta = -0.2 + rng.nextDouble() * 0.4 + bias + boundary;
            }
            double newAvg = Math.max(1.0, Math.min(10.0, avg + delta));
            c.satifySum = newAvg * c.voteSum;
            c.abilitySum = Math.min(10.0, newAvg + rng.nextDouble() * 0.6 - 0.3) * c.voteSum;
            c.lookSum = Math.min(10.0, newAvg + rng.nextDouble() * 0.6 - 0.3) * c.voteSum;
        }

        // 生成全量记录，计算 delta
        List<SatisfactionRecord> records = new ArrayList<>();
        for (Map.Entry<String, CharState> e : chars.entrySet()) {
            String role = e.getKey();
            CharState c = e.getValue();
            double satify = Math.round(c.satifySum / c.voteSum * 10.0) / 10.0;
            Double prev = prevScores.get(role);
            double delta = prev != null ? Math.round((satify - prev) * 10.0) / 10.0 : 0.0;
            prevScores.put(role, satify);

            records.add(new SatisfactionRecord(role, c.star,
                    satify,
                    Math.round(c.abilitySum / c.voteSum * 10.0) / 10.0,
                    Math.round(c.lookSum / c.voteSum * 10.0) / 10.0,
                    c.voteSum, delta));
        }

        records.sort((a, b) -> Double.compare(b.getSatify(), a.getSatify()));
        return records;
    }

    public int getCharCount() { return chars.size(); }
}
