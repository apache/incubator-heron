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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

@SuppressWarnings("unused")
public class DefaultKafkaProducerFactory<K, V> implements KafkaProducerFactory<K, V> {
  private static final long serialVersionUID = -2184222924531815883L;
  private Map<String, Object> configs;

  public DefaultKafkaProducerFactory(Map<String, Object> configs) {
    this.configs = configs;
  }

  @Override
  public Producer<K, V> create() {
    return new KafkaProducer<>(configs);
  }
}
