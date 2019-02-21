package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class DefaultKafkaConsumerFactory<K, V> implements KafkaConsumerFactory<K, V> {
    private static final long serialVersionUID = -2346087278604915148L;
    private transient Map<String, Object> config;

    @SuppressWarnings("WeakerAccess")
    public DefaultKafkaConsumerFactory(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public Consumer<K, V> create() {
        config.put("enable.auto.commit", "false");
        return new KafkaConsumer<>(config);
    }
}
