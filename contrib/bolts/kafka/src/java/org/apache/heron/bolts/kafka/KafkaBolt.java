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
package org.apache.heron.bolts.kafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import static org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;

@SuppressWarnings("unused")
public class KafkaBolt<K, V> extends BaseRichBolt {
  private static final long serialVersionUID = -3301619269473733618L;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);
  private KafkaProducerFactory<K, V> kafkaProducerFactory;
  private TupleTransformer<K, V> tupleTransformer;
  private transient Producer<K, V> producer;
  private Config.TopologyReliabilityMode topologyReliabilityMode;
  private transient OutputCollector outputCollector;

  @SuppressWarnings("WeakerAccess")
  public KafkaBolt(KafkaProducerFactory<K, V> kafkaProducerFactory,
                   TupleTransformer<K, V> tupleTransformer) {
    this.kafkaProducerFactory = kafkaProducerFactory;
    this.tupleTransformer = tupleTransformer;
  }

  @Override
  public void prepare(Map<String, Object> heronConf, TopologyContext context,
                      OutputCollector collector) {
    topologyReliabilityMode = Config.TopologyReliabilityMode
        .valueOf(heronConf.getOrDefault(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE).toString());
    producer = kafkaProducerFactory.create();
    outputCollector = collector;
  }

  @Override
  public void cleanup() {
    super.cleanup();
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void execute(Tuple input) {
    ProducerRecord<K, V> producerRecord = new ProducerRecord<>(
        tupleTransformer.getTopicName(input),
        tupleTransformer.transformToKey(input),
        tupleTransformer.transformToValue(input));
    if (topologyReliabilityMode == Config.TopologyReliabilityMode.EFFECTIVELY_ONCE) {
      try {
        producer.send(producerRecord).get();
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for the record to be sent", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("error has occurred when sending record to Kafka", e);
        throw new KafkaException(e);
      }
    } else {
      producer.send(producerRecord, (recordMetadata, e) -> {
        if (e != null) {
          LOG.error("error has occurred when sending record to Kafka", e);
          if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATLEAST_ONCE) {
            outputCollector.fail(input);
          }
        } else if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATLEAST_ONCE) {
          outputCollector.ack(input);
        }
      });
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //Kafka bolt does not emit anything
  }
}
