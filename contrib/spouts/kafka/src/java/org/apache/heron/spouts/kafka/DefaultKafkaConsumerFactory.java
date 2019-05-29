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

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * a simple Kafka Consumer factory that builds a KafkaConsumer instance from a {@link Map} as
 * the properties to configure it.
 *
 * @param <K> the type of the key of the Kafka record
 * @param <V> the type of the value of the Kafka record
 */
public class DefaultKafkaConsumerFactory<K, V> implements KafkaConsumerFactory<K, V> {
  private static final long serialVersionUID = -2346087278604915148L;
  private Map<String, Object> config;

  /**
   * the config map, key strings should be from {@link ConsumerConfig}
   *
   * @param config the configuration map
   * @see <a href="https://kafka.apache.org/documentation/#consumerconfigs">Kafka Consumer Configs</a>
   */
  public DefaultKafkaConsumerFactory(Map<String, Object> config) {
    this.config = config;
  }

  @Override
  public Consumer<K, V> create() {
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return new KafkaConsumer<>(config);
  }
}
