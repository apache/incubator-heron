/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka.sample;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.simulator.Simulator;
import org.apache.heron.spouts.kafka.DefaultKafkaConsumerFactory;
import org.apache.heron.spouts.kafka.KafkaConsumerFactory;
import org.apache.heron.spouts.kafka.KafkaSpout;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public final class HeronKafkaSpoutSampleTopology {
  private static final Logger LOG = LoggerFactory.getLogger(HeronKafkaSpoutSampleTopology.class);
  private static final String KAFKA_SPOUT_NAME = "kafka-spout";
  private static final String LOGGING_BOLT_NAME = "logging-bolt";

  private HeronKafkaSpoutSampleTopology() {
  }

  public static void main(String[] args) {
    Map<String, Object> kafkaConsumerConfig = new HashMap<>();
    kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-kafka-spout");
    kafkaConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    LOG.info("Kafka Consumer Config: {}", kafkaConsumerConfig);

    KafkaConsumerFactory<String, String> kafkaConsumerFactory =
        new DefaultKafkaConsumerFactory<>(kafkaConsumerConfig);

    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout(KAFKA_SPOUT_NAME, new KafkaSpout<>(kafkaConsumerFactory,
        Collections.singletonList("test-topic")));
    topologyBuilder.setBolt(LOGGING_BOLT_NAME, new LoggingBolt()).shuffleGrouping(KAFKA_SPOUT_NAME);
    Config config = new Config();
    config.setNumStmgrs(1);
    config.setContainerCpuRequested(1);
    config.setContainerRamRequested(ByteAmount.fromGigabytes(1));
    config.setContainerDiskRequested(ByteAmount.fromGigabytes(1));

    config.setComponentCpu(KAFKA_SPOUT_NAME, 0.25);
    config.setComponentRam(KAFKA_SPOUT_NAME, ByteAmount.fromMegabytes(256));
    config.setComponentDisk(KAFKA_SPOUT_NAME, ByteAmount.fromMegabytes(512));

    config.setComponentCpu(LOGGING_BOLT_NAME, 0.25);
    config.setComponentRam(LOGGING_BOLT_NAME, ByteAmount.fromMegabytes(256));
    config.setComponentDisk(LOGGING_BOLT_NAME, ByteAmount.fromMegabytes(256));

    Simulator simulator = new Simulator();
    simulator.submitTopology("heron-kafka-spout-sample-topology", config,
        topologyBuilder.createTopology());
  }

  public static class LoggingBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingBolt.class);
    private transient OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> heronConf, TopologyContext context,
                        OutputCollector collector) {
      this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
      LOG.info("{}", input);
      outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //do nothing
    }
  }
}
