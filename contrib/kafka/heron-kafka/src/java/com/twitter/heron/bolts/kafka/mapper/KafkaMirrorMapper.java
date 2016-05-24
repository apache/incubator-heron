package com.twitter.heron.bolts.kafka.mapper;

import com.twitter.heron.api.tuple.Tuple;

import java.util.Map;

/**
 * Maps key and message from tuples produced by KeyValueScheme deserializer in Kafka spout.
 */
public class KafkaMirrorMapper<K, V> implements TupleToKafkaMapper<K, V> {

    private final static String DEFAULT_KAFKA_MESSAGE_FIELD = "bytes";

    private final String kafkaMsgField;

    public KafkaMirrorMapper() {
        this(DEFAULT_KAFKA_MESSAGE_FIELD);
    }

    public KafkaMirrorMapper(final String kafkaMsgField) {
        this.kafkaMsgField = kafkaMsgField;
    }

    @Override
    public K getKeyFromTuple(Tuple tuple) {
        Object kafkaMsg = tuple.contains(kafkaMsgField) ? tuple.getValueByField(kafkaMsgField) : null;
        if (kafkaMsg != null && kafkaMsg instanceof Map) {
            Map<K, V> kvMap = (Map<K, V>) kafkaMsg;
            if (kvMap.size() > 0) {
                return kvMap.keySet().iterator().next();
            }
        }

        return null;
    }

    @Override
    public V getMessageFromTuple(Tuple tuple) {
        Object kafkaMsg = tuple.contains(kafkaMsgField) ? tuple.getValueByField(kafkaMsgField) : null;
        if (kafkaMsg != null) {
            if (kafkaMsg instanceof Map) {
                Map<K, V> kvMap = (Map<K, V>) kafkaMsg;
                if (kvMap.size() > 0) {
                    return kvMap.values().iterator().next();
                }
            } else {
                return (V) kafkaMsg;
            }
        }

        return null;
    }
}
