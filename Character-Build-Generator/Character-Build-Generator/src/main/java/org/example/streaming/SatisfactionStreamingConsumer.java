package org.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.example.satisfaction.SatisfactionRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

/**
 * Spark Streaming: 消费 satisfaction-v1 → Redis satisfaction:*
 * 替换 satisfaction_consumer.py
 *
 * <p>Redis keys:
 * <ul>
 *   <li>{@code rt:satisfaction:ranking} — ZSET, role→score</li>
 *   <li>{@code rt:satisfaction:trend:{role}} — LIST, RPUSH+LTRIM 60</li>
 *   <li>{@code satisfaction:top} — STRING JSON, TOP6 with delta+trend</li>
 * </ul>
 *
 * <p>运行方式:
 * <pre>
 * spark-submit --class org.example.streaming.SatisfactionStreamingConsumer \
 *   --master spark://master0:7077 --driver-memory 1g --executor-memory 1g \
 *   --jars /root/jedis-3.3.0.jar,/root/kafka2/libs/kafka-clients-2.1.0.jar \
 *   Character-Build-Generator-1.0-SNAPSHOT.jar \
 *   Middleware:9092 satisfaction-v1 Middleware 6379 3
 * </pre>
 */
public class SatisfactionStreamingConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    // 缓存角色 star/ability/look（来自最新一条消息）
    private static final Map<String, RoleCache> CACHE = new HashMap<>();
    // 上轮 satify 分数，用于计算 delta
    private static final Map<String, Double> PREV = new HashMap<>();

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = args.length >= 1 ? args[0] : "Middleware:9092";
        String topic            = args.length >= 2 ? args[1] : "satisfaction-v1";
        String redisHost        = args.length >= 3 ? args[2] : "Middleware";
        int    redisPort        = args.length >= 4 ? Integer.parseInt(args[3]) : 6379;
        int    batchInterval    = args.length >= 5 ? Integer.parseInt(args[4]) : 3;

        SparkConf conf = new SparkConf()
                .setAppName("SatisfactionStreaming")
                .setIfMissing("spark.master", "local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(batchInterval));
        jssc.checkpoint("/tmp/spark-sat-v1-checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", "spark-satisfaction-v1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "false");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

        // Parse JSON
        JavaDStream<SatisfactionRecord> records = kafkaStream.map(record -> {
            try { return MAPPER.readValue(record.value(), SatisfactionRecord.class); }
            catch (Exception e) { return null; }
        }).filter(Objects::nonNull);

        records.foreachRDD(rdd -> {
            List<SatisfactionRecord> all = rdd.collect();
            if (all.isEmpty()) return;

            try (Jedis jedis = new Jedis(redisHost, redisPort)) {
                // ① 处理每条消息：ZADD + RPUSH+LTRIM + 缓存
                for (SatisfactionRecord r : all) {
                    String role = r.getRole() != null ? r.getRole() : "?";
                    jedis.zadd("rt:satisfaction:ranking", r.getSatify(), role);
                    jedis.rpush("rt:satisfaction:trend:" + role, String.valueOf(r.getSatify()));
                    jedis.ltrim("rt:satisfaction:trend:" + role, -60, -1);
                    CACHE.put(role, new RoleCache(r.getStar(), r.getAbility(), r.getLook()));
                }

                // ② 写 TOP6 快照
                Set<redis.clients.jedis.Tuple> top = jedis.zrevrangeWithScores("rt:satisfaction:ranking", 0, 5);
                List<Map<String, Object>> topList = new ArrayList<>();
                for (redis.clients.jedis.Tuple t : top) {
                    String role = t.getElement();
                    double satify = t.getScore();
                    double delta = 0;
                    if (PREV.containsKey(role)) {
                        delta = Math.round((satify - PREV.get(role)) * 10.0) / 10.0;
                    }
                    PREV.put(role, satify);

                    RoleCache cache = CACHE.getOrDefault(role, new RoleCache(5, satify, satify));
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("role", role);
                    item.put("satify", satify);
                    item.put("delta", delta);
                    item.put("star", cache.star);
                    item.put("ability", cache.ability);
                    item.put("look", cache.look);

                    // trend 最近22点
                    List<String> trendVals = jedis.lrange("rt:satisfaction:trend:" + role, -22, -1);
                    List<Double> trend = new ArrayList<>();
                    for (String tv : trendVals) {
                        try { trend.add(Double.parseDouble(tv)); }
                        catch (NumberFormatException ignored) {}
                    }
                    item.put("trend", trend);

                    topList.add(item);
                }
                jedis.set("satisfaction:top", MAPPER.writeValueAsString(topList));
            } catch (Exception e) {
                System.err.println("[Satisfaction] Redis error: " + e.getMessage());
            }
        });

        // Commit offsets
        kafkaStream.foreachRDD(rdd -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsets);
        });

        jssc.start();
        System.out.println("[SatisfactionStreaming] 启动成功 topic=" + topic + " batch=" + batchInterval + "s");
        jssc.awaitTermination();
    }

    static class RoleCache {
        final int star; final double ability; final double look;
        RoleCache(int star, double ability, double look) {
            this.star = star; this.ability = ability; this.look = look;
        }
    }
}
