package com.twitter.heron.spouts.kafka;

import com.twitter.heron.spouts.kafka.common.Broker;
import com.twitter.heron.spouts.kafka.common.GlobalPartitionInformation;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Reads Kafka cluster info from Kafka coordinator server using arbitrary consumer group consumer.
 */
public class DynamicKafkaBrokerReader {

    private final String topic;

    private final KafkaConsumer<byte[], byte[]> consumer;

    private static final Logger LOG = LoggerFactory.getLogger(DynamicKafkaBrokerReader.class);

    public DynamicKafkaBrokerReader(String topic, String bootstrapBrokers) {
        this.topic = topic;

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", bootstrapBrokers);
        kafkaProps.put("group.id", UUID.randomUUID().toString()); // because we don't want it to mess with any existing consumer group
        kafkaProps.put("enable.auto.commit", "false");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.consumer = new KafkaConsumer<>(kafkaProps);
    }

    public GlobalPartitionInformation getBrokerInfo() {
        List<PartitionInfo> partitionInfo = consumer.partitionsFor(topic);
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(topic);
        for (PartitionInfo singlePartition : partitionInfo) {
            Node leader = singlePartition.leader();
            globalPartitionInformation.addPartition(singlePartition.partition(), new Broker(leader.host(), leader.port()));
        }

        LOG.info("Read partition info from Kafka: " + globalPartitionInformation);
        return globalPartitionInformation;
    }

    public void close() {
        consumer.close();
    }
}
