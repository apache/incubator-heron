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

package org.apache.heron.spouts.kafka;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.junit.Assert.assertTrue;

public class DefaultKafkaConsumerFactoryTest {
  private KafkaConsumerFactory<String, byte[]> kafkaConsumerFactory;

  @Before
  public void setUp() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("group.id", "tower-kafka-spout");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(config);
  }

  @Test
  public void create() {
    try (Consumer<String, byte[]> consumer = kafkaConsumerFactory.create()) {
      assertTrue(consumer instanceof KafkaConsumer);
    }
  }
}
