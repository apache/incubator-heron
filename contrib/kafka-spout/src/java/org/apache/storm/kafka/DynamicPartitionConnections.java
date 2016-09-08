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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.kafka.trident.IBrokerReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.consumer.SimpleConsumer;


public class DynamicPartitionConnections {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);

  static class ConnectionInfo {
    public final SimpleConsumer consumer;
    public final Set<String> partitions = new HashSet<>();

    ConnectionInfo(SimpleConsumer consumer) {
      this.consumer = consumer;
    }
  }

  private Map<Broker, ConnectionInfo> connections = new HashMap<>();
  private KafkaConfig config;
  private IBrokerReader reader;

  public DynamicPartitionConnections(KafkaConfig config, IBrokerReader brokerReader) {
    this.config = config;
    this.reader = brokerReader;
  }

  public SimpleConsumer register(Partition partition) {
    Broker broker = reader.getBrokerForTopic(partition.topic).getBrokerFor(partition.partition);
    return register(broker, partition.topic, partition.partition);
  }

  public SimpleConsumer register(Broker host, String topic, int partition) {
    if (!connections.containsKey(host)) {
      connections.put(host,
          new ConnectionInfo(
              new SimpleConsumer(host.host,
                  host.port,
                  config.socketTimeoutMs,
                  config.bufferSizeBytes,
                  config.clientId)));
    }
    ConnectionInfo info = connections.get(host);
    info.partitions.add(getHashKey(topic, partition));
    return info.consumer;
  }

  public SimpleConsumer getConnection(Partition partition) {
    ConnectionInfo info = connections.get(partition.host);
    if (info != null) {
      return info.consumer;
    }
    return null;
  }

  public void unregister(Broker port, String topic, int partition) {
    ConnectionInfo info = connections.get(port);
    info.partitions.remove(getHashKey(topic, partition));
    if (info.partitions.isEmpty()) {
      info.consumer.close();
      connections.remove(port);
    }
  }

  public void unregister(Partition partition) {
    unregister(partition.host, partition.topic, partition.partition);
  }

  public void clear() {
    for (ConnectionInfo info : connections.values()) {
      info.consumer.close();
    }
    connections.clear();
  }

  private String getHashKey(String topic, int partition) {
    return topic + "_" + partition;
  }
}
