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
package org.apache.storm.kafka;

import java.util.HashMap;
import java.util.Map;

import kafka.javaapi.consumer.SimpleConsumer;

public class StaticPartitionConnections {
  private Map<Integer, SimpleConsumer> kafka = new HashMap<Integer, SimpleConsumer>();
  private KafkaConfig config;
  private StaticHosts hosts;

  public StaticPartitionConnections(KafkaConfig conf) {
    config = conf;
    if (!(conf.hosts instanceof StaticHosts)) {
      throw new RuntimeException("Must configure with static hosts");
    }
    this.hosts = (StaticHosts) conf.hosts;
  }

  public SimpleConsumer getConsumer(int partition) {
    if (!kafka.containsKey(partition)) {
      Broker hp = hosts.getPartitionInformation().getBrokerFor(partition);
      kafka.put(partition, new SimpleConsumer(hp.host,
          hp.port, config.socketTimeoutMs, config.bufferSizeBytes, config.clientId));

    }
    return kafka.get(partition);
  }

  public void close() {
    for (SimpleConsumer consumer : kafka.values()) {
      consumer.close();
    }
  }
}
