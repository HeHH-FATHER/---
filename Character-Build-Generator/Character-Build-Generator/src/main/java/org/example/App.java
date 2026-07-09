package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.generator.CharacterBuildGenerator;
import org.example.model.CharacterBuildRecord;
import org.example.model.CharacterBuildStats;
import org.example.publisher.KafkaRecordPublisher;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Character Build Generator.
 *
 * <p>File output mode:
 * <pre>
 *   java -jar ... &lt;stats.json&gt; [--count N] [--out-dir &lt;dir&gt;] [--seed N]
 * </pre>
 *
 * <p>Kafka mode (single batch):
 * <pre>
 *   java -jar ... &lt;stats.json&gt; --kafka-bootstrap-servers localhost:9092 [--kafka-topic character-builds]
 * </pre>
 *
 * <p>Kafka continuous mode (loop inside JVM, KafkaProducer singleton):
 * <pre>
 *   java -jar ... &lt;stats.json&gt; --kafka-bootstrap-servers localhost:9092 --loop [--interval 1] [--count 10]
 * </pre>
 */
public class App {

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        try {
            // ── parse arguments ──────────────────────────────────────────
            String inputPath = args[0];
            int maxRecords = 0;
            String outDir = null;
            Long seed = null;
            String kafkaBootstrap = null;
            String kafkaTopic = "character-builds";
            boolean kafkaSync = false;
            boolean loop = false;
            int intervalSec = 1;

            for (int i = 1; i < args.length; i++) {
                switch (args[i]) {
                    case "--count":        maxRecords = Integer.parseInt(args[++i]); break;
                    case "--out-dir":      outDir = args[++i]; break;
                    case "--seed":         seed = Long.parseLong(args[++i]); break;
                    case "--kafka-bootstrap-servers": kafkaBootstrap = args[++i]; break;
                    case "--kafka-topic":            kafkaTopic = args[++i]; break;
                    case "--kafka-sync":             kafkaSync = true; break;
                    case "--loop":         loop = true; break;
                    case "--interval":     intervalSec = Integer.parseInt(args[++i]); break;
                }
            }

            if (kafkaBootstrap == null && outDir == null) {
                outDir = "character_builds";
            }

            // ── read input ────────────────────────────────────────────────
            ObjectMapper mapper = new ObjectMapper();
            System.out.println("Reading build statistics from: " + inputPath);
            CharacterBuildStats[] statsArray = mapper.readValue(
                    new File(inputPath), CharacterBuildStats[].class);
            List<CharacterBuildStats> statsList = Arrays.asList(statsArray);

            int totalPlayers = statsList.stream()
                    .mapToInt(CharacterBuildStats::getPlayer_count).sum();
            System.out.printf("Loaded %d characters, %,d total players in statistics.%n",
                    statsList.size(), totalPlayers);

            // ── continuous loop mode ──────────────────────────────────────
            if (loop && kafkaBootstrap != null) {
                runContinuousLoop(statsList, maxRecords, seed,
                        kafkaBootstrap, kafkaTopic, intervalSec, mapper);
                return;
            }

            // ── single-shot mode ──────────────────────────────────────────
            CharacterBuildGenerator generator = seed != null
                    ? new CharacterBuildGenerator(seed)
                    : new CharacterBuildGenerator();

            long start = System.currentTimeMillis();
            List<CharacterBuildRecord> records = generator.generate(statsList, maxRecords);
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("Generated %,d records in %,d ms.%n", records.size(), elapsed);
            System.out.println(CharacterBuildGenerator.summarize(records));

            if (outDir != null) {
                writeFiles(records, outDir, mapper);
            }

            if (kafkaBootstrap != null) {
                publishToKafka(records, kafkaBootstrap, kafkaTopic, kafkaSync, mapper);
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Error: invalid number — " + e.getMessage());
            System.exit(1);
        }
    }

    // ── continuous loop: KafkaProducer singleton ──────────────────────────

    private static void runContinuousLoop(List<CharacterBuildStats> statsList,
                                          int countPerBatch, Long seed,
                                          String bootstrap, String topic,
                                          int intervalSec, ObjectMapper mapper) {
        CharacterBuildGenerator generator = seed != null
                ? new CharacterBuildGenerator(seed)
                : new CharacterBuildGenerator();

        System.out.printf("Continuous mode: %d records every %ds → Kafka %s/%s%n",
                countPerBatch, intervalSec, bootstrap, topic);
        System.out.println("Press Ctrl+C to stop.");

        try (KafkaRecordPublisher publisher = new KafkaRecordPublisher(bootstrap, topic)) {
            long batch = 0;
            while (true) {
                batch++;
                long start = System.currentTimeMillis();
                List<CharacterBuildRecord> records = generator.generate(statsList, countPerBatch);

                publisher.publishAsync(records);
                publisher.flush();

                long elapsed = System.currentTimeMillis() - start;
                System.out.printf("[%d] Published %,d records in %,d ms%n",
                        batch, records.size(), elapsed);

                Thread.sleep(intervalSec * 1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Continuous loop stopped.");
        }
    }

    // ── file output ───────────────────────────────────────────────────────

    private static void writeFiles(List<CharacterBuildRecord> records, String outDir,
                                   ObjectMapper mapper) throws IOException {
        Path dir = Paths.get(outDir);
        Files.createDirectories(dir);

        File[] existing = dir.toFile().listFiles();
        if (existing != null) {
            for (File f : existing) {
                if (f.getName().endsWith(".json")) f.delete();
            }
        }

        for (int i = 0; i < records.size(); i++) {
            CharacterBuildRecord r = records.get(i);
            String json = mapper.writeValueAsString(r);
            String filename = String.format("%06d_%s_%s.json",
                    i + 1, r.getUid(), sanitizeFilename(r.getRole()));
            Files.write(dir.resolve(filename), json.getBytes(StandardCharsets.UTF_8));
        }

        System.out.printf("Wrote %,d record files to: %s%n",
                records.size(), dir.toAbsolutePath());
    }

    // ── Kafka single-shot ─────────────────────────────────────────────────

    private static void publishToKafka(List<CharacterBuildRecord> records,
                                       String bootstrap, String topic, boolean sync,
                                       ObjectMapper mapper) {
        System.out.printf("Publishing %,d records to Kafka (%s, topic=%s)...%n",
                records.size(), bootstrap, topic);

        long start = System.currentTimeMillis();
        try (KafkaRecordPublisher publisher = new KafkaRecordPublisher(bootstrap, topic)) {
            if (sync) {
                int sent = publisher.publishSync(records);
                System.out.printf("Sent %d/%d records to Kafka in %,d ms.%n",
                        sent, records.size(), System.currentTimeMillis() - start);
            } else {
                publisher.publishAsync(records);
                publisher.flush();
                System.out.printf("Published %,d records to Kafka in %,d ms.%n",
                        records.size(), System.currentTimeMillis() - start);
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static String sanitizeFilename(String s) {
        return s.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

    private static void printUsage() {
        System.out.println("Character Build Generator");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar Character-Build-Generator.jar <build-stats.json> [options]");
        System.out.println();
        System.out.println("File output options:");
        System.out.println("  --out-dir DIR        Output directory (default: ./character_builds)");
        System.out.println();
        System.out.println("Kafka single-shot:");
        System.out.println("  --kafka-bootstrap-servers HOST:PORT");
        System.out.println("  --kafka-topic TOPIC  (default: character-builds)");
        System.out.println("  --kafka-sync         Wait for broker ack per message");
        System.out.println();
        System.out.println("Kafka continuous loop:");
        System.out.println("  --loop               Run continuously (requires --kafka-bootstrap-servers)");
        System.out.println("  --interval N         Seconds between batches (default: 1)");
        System.out.println();
        System.out.println("General options:");
        System.out.println("  --count N            Records per batch (default: proportional to player_count)");
        System.out.println("  --seed N             RNG seed for reproducible output");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Single batch → Kafka");
        System.out.println("  java -jar ... build_stats.json --kafka-bootstrap-servers Middleware:9092 --count 100");
        System.out.println();
        System.out.println("  # Continuous → Kafka (10 records/sec, recommended)");
        System.out.println("  java -jar ... build_stats.json --kafka-bootstrap-servers Middleware:9092 --loop --count 10 --interval 1");
    }
}
