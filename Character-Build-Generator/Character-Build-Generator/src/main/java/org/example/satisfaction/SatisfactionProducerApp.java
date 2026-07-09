package org.example.satisfaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;

/**
 * 满意度实时生成器 — 主程序
 *
 * Kafka 连续模式:
 *   java -cp ... SatisfactionProducerApp 角色满意度排行.json
 *        --kafka-bootstrap-servers Middleware:9092 --kafka-topic satisfaction-v1 --loop --interval 3
 *
 * File 模式:
 *   java -cp ... SatisfactionProducerApp 角色满意度排行.json --out-dir output/
 */
public class SatisfactionProducerApp {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("用法: SatisfactionProducerApp <满意度JSON> [--loop] [--interval N] [--kafka-bootstrap-servers HOST:PORT] [--kafka-topic TOPIC] [--out-dir dir]");
            System.exit(1);
        }

        String dataPath = args[0];
        boolean loop = hasArg(args, "--loop");
        int interval = intArg(args, "--interval", 3);
        String kafkaServers = strArg(args, "--kafka-bootstrap-servers");
        String kafkaTopic = strArg(args, "--kafka-topic");
        String outDir = strArg(args, "--out-dir");

        SatisfactionGenerator gen = new SatisfactionGenerator(dataPath);
        ObjectMapper mapper = new ObjectMapper();

        // Kafka 生产者（自含，不依赖 KRP）
        KafkaProducer<String, String> kafkaProducer = null;
        if (kafkaServers != null && kafkaTopic != null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServers);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            kafkaProducer = new KafkaProducer<>(props);
        }

        System.out.println("Satisfaction Producer 启动, " + gen.getCharCount() + " 角色");
        if (loop) System.out.println("循环模式, 间隔 " + interval + "s");
        if (kafkaProducer != null) System.out.println("Kafka: " + kafkaServers + " / " + kafkaTopic);

        int batch = 0;
        do {
            List<SatisfactionRecord> records = gen.tick();
            // 发送全部 121 角色（保证每个角色都有趋势数据）
            for (SatisfactionRecord r : records) {
                String json = mapper.writeValueAsString(r);

                if (kafkaProducer != null) {
                    kafkaProducer.send(new ProducerRecord<>(kafkaTopic, r.getRole(), json));
                }

                if (outDir != null) {
                    Files.write(
                        Paths.get(outDir, "satisfaction_" + batch + ".jsonl"),
                        (json + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            }

            if (kafkaProducer != null) kafkaProducer.flush();

            String names = "";
            for (int i = 0; i < Math.min(3, records.size()); i++)
                names += records.get(i).getRole() + "(" + records.get(i).getSatify() + ") ";
            System.out.println("[Satisfaction-" + (++batch) + "] " + names);

            if (!loop) break;
            Thread.sleep(interval * 1000L);
        } while (loop);

        if (kafkaProducer != null) kafkaProducer.close();
        System.out.println("完成, " + batch + " 批");
    }

    private static boolean hasArg(String[] args, String key) { for (String a : args) if (key.equals(a)) return true; return false; }
    private static String strArg(String[] args, String key) { for (int i=0; i<args.length-1; i++) if (key.equals(args[i])) return args[i+1]; return null; }
    private static int intArg(String[] args, String key, int def) { String v=strArg(args,key); return v!=null ? Integer.parseInt(v) : def; }
}
