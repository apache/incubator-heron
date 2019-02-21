package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultKafkaConsumerFactoryTest {
    private KafkaConsumerFactory<String, byte[]> kafkaConsumerFactory;

    @BeforeEach
    void setUp() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "tower-kafka-spout");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(config);
    }

    @Test
    void create() {
        try (Consumer<String, byte[]> consumer = kafkaConsumerFactory.create()) {
            assertTrue(consumer instanceof KafkaConsumer);
        }
    }
}