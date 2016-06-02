// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron;

import java.util.Properties;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.bolts.kafka.KafkaBolt;
import com.twitter.heron.bolts.kafka.mapper.KafkaMirrorMapper;
import com.twitter.heron.spouts.kafka.common.ByteArrayKeyValueScheme;
import com.twitter.heron.spouts.kafka.common.KeyValueSchemeAsMultiScheme;
import com.twitter.heron.spouts.kafka.old.KafkaSpout;
import com.twitter.heron.spouts.kafka.old.SpoutConfig;

public final class KafkaOldMirrorTopology {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new RuntimeException("Two arguments required:\n"
                                 + "0 - Topology name\n"
                                 + "1 - Bootstrap Kafka servers");
    }

    String bootstrapKafkaServers = args[1];
    String consumerTopic = "foo";
    String producerTopic = "foo_mirrored";
    if (args.length > 3) {
      consumerTopic = args[2];
      producerTopic = args[3];
    }
    String zkURL = "master:2181";
    String zkRoot = "/brokers";
    if (args.length > 5) {
      zkURL = args[4];
      zkRoot = args[5] + zkRoot;
    }
    TopologyBuilder builder = new TopologyBuilder();
    SpoutConfig config = new SpoutConfig(new SpoutConfig.ZkHosts(zkURL, zkRoot),
                                            consumerTopic, null, "spoutId");

    config.scheme = new KeyValueSchemeAsMultiScheme(new ByteArrayKeyValueScheme());
    config.bufferSizeBytes = 100; // Don't need buffer to be too big for showcase purposes
    builder.setSpout("spout", new KafkaSpout(config), 1);
    Properties kafkaBoltProps = new Properties();
    kafkaBoltProps.put("acks", "1");
    kafkaBoltProps.put("bootstrap.servers", bootstrapKafkaServers);
    kafkaBoltProps.put("key.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    kafkaBoltProps.put("value.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    kafkaBoltProps.put("metadata.fetch.timeout.ms", 1000);
    builder.setBolt("bolt",
        new KafkaBolt<byte[], byte[]>(producerTopic)
            .withProducerProperties(kafkaBoltProps)
            .withTupleToKafkaMapper(new KafkaMirrorMapper<byte[], byte[]>()),
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

  private KafkaOldMirrorTopology() {

  }
}
