package com.twitter.heron;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.bolts.kafka.KafkaBolt;
import com.twitter.heron.bolts.kafka.mapper.KafkaMirrorMapper;
import com.twitter.heron.spouts.kafka.common.ByteArrayKeyValueScheme;
import com.twitter.heron.spouts.kafka.common.KeyValueSchemeAsMultiScheme;
import com.twitter.heron.spouts.kafka.KafkaSpout;
import com.twitter.heron.spouts.kafka.SpoutConfig;

import java.util.Properties;

public class KafkaMirrorTopology {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("Would need at least two arguments:\n 0 - Topology name\n 1 - Bootstrap Kafka servers");
        }

        String bootstrapKafkaServers = args[1];
        String consumerTopic = "foo";
        String producerTopic = "foo_mirrored";
        if (args.length > 3) {
            consumerTopic = args[2];
            producerTopic = args[3];
        }
        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig config = new SpoutConfig(consumerTopic, bootstrapKafkaServers, "spoutId");
        config.scheme = new KeyValueSchemeAsMultiScheme(new ByteArrayKeyValueScheme());
        builder.setSpout("spout", new KafkaSpout(config), 1);
        Properties kafkaBoltProps = new Properties();
        kafkaBoltProps.put("acks", "1");
        kafkaBoltProps.put("bootstrap.servers", bootstrapKafkaServers);
        kafkaBoltProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaBoltProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaBoltProps.put("metadata.fetch.timeout.ms", 1000);
        builder.setBolt("bolt",
                new KafkaBolt<byte[], byte[]>(producerTopic)
                        .withProducerProperties(kafkaBoltProps)
                        .withTupleToKafkaMapper(new KafkaMirrorMapper<>()),
                1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setComponentRam("spout", 1024L * 1024 * 256);
        conf.setComponentRam("bolt", 1024L * 1024 * 256);

        conf.setNumStmgrs(1);
        conf.setContainerCpuRequested(0.2f);
        conf.setContainerRamRequested(1024L * 1024 * 512);
        conf.setContainerDiskRequested(1024L * 1024 * 1024);

        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
