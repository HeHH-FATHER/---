package org.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.example.model.CharacterBuildRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

/**
 * Spark Streaming: 消费 build-v2 → Redis build:recent + build:hot_chars
 * 替换 realtime_consumer.py(build线程) + hot_char_aggregator.py
 *
 * <p>Redis keys:
 * <ul>
 *   <li>{@code build:recent} — LIST, LPUSH JSON + LTRIM 20（最新20条）</li>
 *   <li>{@code build:hot_chars} — STRING JSON, 60s窗口按角色聚合TOP4</li>
 * </ul>
 *
 * <p>运行方式:
 * <pre>
 * spark-submit --class org.example.streaming.CharacterBuildStreamingConsumer \
 *   --master spark://master0:7077 --driver-memory 1g --executor-memory 1g \
 *   --jars /root/jedis-3.3.0.jar,/root/kafka2/libs/kafka-clients-2.1.0.jar \
 *   Character-Build-Generator-1.0-SNAPSHOT.jar \
 *   Middleware:9092 build-v2 Middleware 6379 3
 * </pre>
 */
public class CharacterBuildStreamingConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int RECENT_SIZE = 20;
    private static final int TOP_N = 4;

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = args.length >= 1 ? args[0] : "Middleware:9092";
        String topic            = args.length >= 2 ? args[1] : "build-v2";
        String redisHost        = args.length >= 3 ? args[2] : "Middleware";
        int    redisPort        = args.length >= 4 ? Integer.parseInt(args[3]) : 6379;
        int    batchInterval    = args.length >= 5 ? Integer.parseInt(args[4]) : 3;

        SparkConf conf = new SparkConf()
                .setAppName("BuildStreaming")
                .setIfMissing("spark.master", "local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(batchInterval));
        jssc.checkpoint("/tmp/spark-build-v2-checkpoint");

        // Kafka params
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
                kafkaParams.put("group.id", "build-" + System.currentTimeMillis()/1000);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "false");

        // Direct stream
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

        // Parse JSON
        JavaDStream<CharacterBuildRecord> records = kafkaStream.map(record -> {
            try { return MAPPER.readValue(record.value(), CharacterBuildRecord.class); }
            catch (Exception e) { return null; }
        }).filter(Objects::nonNull);

        // ── ① build:recent — per-batch LPUSH + LTRIM ──────────────────────
        records.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                JedisPool pool = new JedisPool(redisHost, redisPort);
                try (Jedis jedis = pool.getResource()) {
                    while (partition.hasNext()) {
                        CharacterBuildRecord r = partition.next();
                        Map<String, Object> rec = toRecentJson(r);
                        jedis.lpush("build:recent", MAPPER.writeValueAsString(rec));
                        jedis.ltrim("build:recent", 0, RECENT_SIZE - 1); // 逐条trim，保证实时滚动
                    }
                } catch (Exception e) {
                    System.err.println("[Build] Redis error: " + e.getMessage());
                } finally { pool.close(); }
            });
        });

        // ── ② build:hot_chars — 60s滑动窗口按角色聚合 ─────────────────────
        JavaDStream<CharacterBuildRecord> windowed =
                records.window(Durations.seconds(60), Durations.seconds(3));

        windowed.foreachRDD(rdd -> {
            List<CharacterBuildRecord> all = rdd.collect();
            if (all.isEmpty()) return;

            // 按角色聚合
            Map<String, CharAgg> aggs = new HashMap<>();
            for (CharacterBuildRecord r : all) {
                String role = r.getRole() != null ? r.getRole() : "?";
                CharAgg agg = aggs.computeIfAbsent(role, k -> new CharAgg());
                agg.count++;
                agg.star = r.getStar();
                agg.constellationSum += r.getConstellation();
                agg.damageSum += r.getAvg_damage();
                agg.ename = r.getEname() != null ? r.getEname() : "";
                // 武器
                if (r.getWeapon() != null && r.getWeapon().getName() != null) {
                    String wn = r.getWeapon().getName();
                    agg.weapons.merge(wn, 1, Integer::sum);
                }
                // 圣遗物
                if (r.getArtifact_set() != null && r.getArtifact_set().getName() != null) {
                    String an = r.getArtifact_set().getName();
                    agg.artifacts.merge(an, 1, Integer::sum);
                }
            }

            // TOP N 按 count 排序
            List<Map.Entry<String, CharAgg>> sorted = new ArrayList<>(aggs.entrySet());
            sorted.sort((a, b) -> Integer.compare(b.getValue().count, a.getValue().count));
            int limit = Math.min(TOP_N, sorted.size());

            List<Map<String, Object>> result = new ArrayList<>();
            for (int i = 0; i < limit; i++) {
                Map.Entry<String, CharAgg> e = sorted.get(i);
                CharAgg agg = e.getValue();
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("role", e.getKey());
                item.put("star", agg.star);
                item.put("count", agg.count);
                item.put("avg_constellation", round1(agg.constellationSum / (double) agg.count));
                item.put("avg_damage", (int) (agg.damageSum / agg.count));
                item.put("ename", agg.ename);

                // 武器 TOP1
                String topW = agg.weapons.entrySet().stream()
                        .max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("?");
                int wTotal = agg.weapons.values().stream().mapToInt(Integer::intValue).sum();
                List<Map<String, Object>> weapList = new ArrayList<>();
                agg.weapons.entrySet().stream()
                        .sorted((a2, b2) -> Integer.compare(b2.getValue(), a2.getValue()))
                        .forEach(we -> {
                            Map<String, Object> wm = new LinkedHashMap<>();
                            wm.put("name", we.getKey()); wm.put("count", we.getValue());
                            wm.put("ratio", round2(we.getValue() / (double) wTotal));
                            weapList.add(wm);
                        });
                item.put("weapons", weapList);

                // 圣遗物 TOP1
                String topA = agg.artifacts.entrySet().stream()
                        .max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("?");
                int aTotal = agg.artifacts.values().stream().mapToInt(Integer::intValue).sum();
                List<Map<String, Object>> artiList = new ArrayList<>();
                agg.artifacts.entrySet().stream()
                        .sorted((a2, b2) -> Integer.compare(b2.getValue(), a2.getValue()))
                        .forEach(ae -> {
                            Map<String, Object> am = new LinkedHashMap<>();
                            am.put("name", ae.getKey()); am.put("count", ae.getValue());
                            am.put("ratio", round2(ae.getValue() / (double) aTotal));
                            artiList.add(am);
                        });
                item.put("artifacts", artiList);

                result.add(item);
            }

            // 写 Redis
            try (Jedis jedis = new Jedis(redisHost, redisPort)) {
                jedis.set("build:hot_chars", MAPPER.writeValueAsString(result));
                System.out.println("[Build-HotChar] TOP" + result.size() + ": " +
                        result.stream().map(m -> m.get("role") + "(" + m.get("count") + ")")
                                .reduce((a, b) -> a + ", " + b).orElse(""));
            } catch (Exception ex) {
                System.err.println("[Build-HotChar] Redis error: " + ex.getMessage());
            }
        });

        // Commit offsets
        kafkaStream.foreachRDD(rdd -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsets);
        });

        jssc.start();
        System.out.println("[BuildStreaming] 启动成功 topic=" + topic + " batch=" + batchInterval + "s");
        jssc.awaitTermination();
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Map<String, Object> toRecentJson(CharacterBuildRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("role", r.getRole());
        m.put("star", r.getStar());
        m.put("constellation", r.getConstellation());
        m.put("level", r.getLevel());
        m.put("damage", r.getAvg_damage());
        m.put("weapon", r.getWeapon() != null ? r.getWeapon().getName() : "?");
        m.put("arti", r.getArtifact_set() != null ? r.getArtifact_set().getName() : "?");
        return m;
    }

    private static double round1(double v) { return Math.round(v * 10.0) / 10.0; }
    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    static class CharAgg {
        int count, star, constellationSum;
        long damageSum;
        String ename = "";
        Map<String, Integer> weapons = new HashMap<>();
        Map<String, Integer> artifacts = new HashMap<>();
    }
}
