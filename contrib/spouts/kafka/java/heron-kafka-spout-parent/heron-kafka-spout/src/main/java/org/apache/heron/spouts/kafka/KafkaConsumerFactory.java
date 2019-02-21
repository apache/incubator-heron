package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Serializable;

interface KafkaConsumerFactory<K, V> extends Serializable {
    Consumer<K, V> create();
}
