package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

interface ConsumerRecordTransformer<K, V> extends Serializable {
    default String getOutputStream() {
        return "default";
    }

    default List<String> getFieldNames() {
        return Arrays.asList("key", "value");
    }

    default List<Object> transform(ConsumerRecord<K, V> record) {
        return Arrays.asList(record.key(), record.value());
    }
}
