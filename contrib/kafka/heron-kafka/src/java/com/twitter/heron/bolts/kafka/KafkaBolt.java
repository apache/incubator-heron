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

package com.twitter.heron.bolts.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.bolts.kafka.mapper.TupleToKafkaMapper;

@SuppressWarnings({"rawtypes", "serial"})
// CHECKSTYLE:OFF IllegalCatch
public class KafkaBolt<K, V> extends BaseRichBolt {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);

  private KafkaProducer<K, V> producer;
  private OutputCollector collector;
  public String topic;
  private TupleToKafkaMapper<K, V> mapper;
  private Properties kafkaProperties = new Properties();

  private boolean fireAndForget = false;
  private boolean async = true;

  public KafkaBolt(String topic) {
    if (topic != null) {
      this.topic = topic;
    }
  }

  public KafkaBolt<K, V> withProducerProperties(Properties producerProperties) {
    this.kafkaProperties = producerProperties;
    return this;
  }

  public KafkaBolt<K, V> withTupleToKafkaMapper(TupleToKafkaMapper<K, V> kafkaMapper) {
    this.mapper = kafkaMapper;
    return this;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    producer = new KafkaProducer<>(kafkaProperties);
    this.collector = outputCollector;
  }

  private static boolean isTick(Tuple tuple) {
    return tuple != null && "__system".equals(tuple.getSourceComponent()) && "__tick".equals(tuple.
        getSourceStreamId());
  }

  @Override
  public void execute(final Tuple input) {
    LOG.debug("Got new tuple input, processing..");
    if (isTick(input)) {
      collector.ack(input);
      return; // Do not try to send ticks to Kafka
    }
    K key = null;
    V message = null;
    try {
      key = mapper.getKeyFromTuple(input);
      message = mapper.getMessageFromTuple(input);
      LOG.debug("Successfully parsed a message. Sending..");
      if (topic != null) {
        Callback callback = null;

        if (!fireAndForget && async) {
          callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata ignored, Exception e) {
              synchronized (collector) {
                if (e != null) {
                  LOG.debug("Seems like failed to produced a message:", e);
                  collector.reportError(e);
                  collector.fail(input);
                } else {
                  LOG.debug("Successfully produced a message. Acknowledging..");
                  collector.ack(input);
                }
              }
            }
          };
        }
        Future<RecordMetadata> result = producer.send(new ProducerRecord<K, V>(topic, key, message),
            callback);
        if (!async) {
          try {
            result.get();
            collector.ack(input);
          } catch (ExecutionException err) {
            collector.reportError(err);
            collector.fail(input);
          }
        } else if (fireAndForget) {
          collector.ack(input);
        }
      } else {
        //LOG.warn("skipping key = " + key + ", topic selector returned null.");
        collector.ack(input);
      }
    } catch (Exception ex) {
      LOG.warn("An unexpected error occurred: " + ex);
      collector.reportError(ex);
      collector.fail(input);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public void cleanup() {
    producer.close();
  }

  public void setFireAndForget(boolean fireAndForget) {
    this.fireAndForget = fireAndForget;
  }

  public void setAsync(boolean async) {
    this.async = async;
  }
}
