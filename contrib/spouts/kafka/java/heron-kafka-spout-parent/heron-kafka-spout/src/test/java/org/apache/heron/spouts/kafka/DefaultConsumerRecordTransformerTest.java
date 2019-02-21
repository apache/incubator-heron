package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultConsumerRecordTransformerTest {
    private ConsumerRecordTransformer<String, byte[]> consumerRecordTransformer;

    @BeforeEach
    void setUp() {
        consumerRecordTransformer = new DefaultConsumerRecordTransformer<>();
    }

    @Test
    void getOutputStreams() {
        assertEquals("default", consumerRecordTransformer.getOutputStream());
    }

    @Test
    void getFieldNames() {
        assertEquals(Arrays.asList("key", "value"), consumerRecordTransformer.getFieldNames());
    }

    @Test
    void transform() {
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>("partition", 0, 0, "key", new byte[]{0x1, 0x2, 0x3});
        List<Object> expected = Arrays.asList(consumerRecord.key(), consumerRecord.value());
        assertEquals(expected, consumerRecordTransformer.transform(consumerRecord));
    }
}