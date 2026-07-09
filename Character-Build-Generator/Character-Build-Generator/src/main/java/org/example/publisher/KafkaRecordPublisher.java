package org.example.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.CharacterBuildRecord;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes character build records to a Kafka topic.
 *
 * <p>Each record is sent as a JSON message. The message key is the player UID,
 * so all builds by the same player land on the same partition (ordering guarantee).
 */
public class KafkaRecordPublisher implements Closeable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper mapper;
    private final long sendTimeoutMs;

    public KafkaRecordPublisher(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, 10_000);
    }

    public KafkaRecordPublisher(String bootstrapServers, String topic, long sendTimeoutMs) {
        this.topic = topic;
        this.mapper = new ObjectMapper();
        this.sendTimeoutMs = sendTimeoutMs;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safety: prevent data loss
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Batching for throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        // Idempotent producer (no duplicates on retry)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Publish records synchronously (waits for all acks).
     * @return number of records successfully sent
     */
    public int publishSync(List<CharacterBuildRecord> records) {
        int sent = 0;
        for (CharacterBuildRecord record : records) {
            try {
                sendOne(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                sent++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted during Kafka send");
                break;
            } catch (ExecutionException | TimeoutException e) {
                System.err.println("Kafka send failed for " + record.getUid()
                        + "/" + record.getRole() + ": " + e.getMessage());
            }
        }
        producer.flush();
        return sent;
    }

    /**
     * Publish records asynchronously (fire-and-forget with error logging).
     */
    public void publishAsync(List<CharacterBuildRecord> records) {
        for (CharacterBuildRecord record : records) {
            sendOne(record);
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    // ── internals ─────────────────────────────────────────────────────────

    private Future<RecordMetadata> sendOne(CharacterBuildRecord record) {
        try {
            String key = record.getUid();
            String value = mapper.writeValueAsString(record);
            return producer.send(new ProducerRecord<>(topic, key, value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Kafka async send error: " + exception.getMessage());
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize record", e);
        }
    }
}
