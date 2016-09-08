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
package org.apache.storm.kafka.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.kafka.DynamicBrokersReader;
import org.apache.storm.kafka.ZkHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBrokerReader implements IBrokerReader {

  private static final Logger LOG = LoggerFactory.getLogger(ZkBrokerReader.class);

  private List<GlobalPartitionInformation> cachedBrokers =
      new ArrayList<GlobalPartitionInformation>();
  private DynamicBrokersReader reader;
  private long lastRefreshTimeMs;
  private long refreshMillis;

  public ZkBrokerReader(Map<String, Object> conf, String topic, ZkHosts hosts) {
    try {
      reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
      cachedBrokers = reader.getBrokerInfo();
      lastRefreshTimeMs = System.currentTimeMillis();
      refreshMillis = hosts.refreshFreqSecs * 1000L;
    } catch (java.net.SocketTimeoutException e) {
      LOG.warn("Failed to update brokers", e);
    }

  }

  private void refresh() {
    long currTime = System.currentTimeMillis();
    if (currTime > lastRefreshTimeMs + refreshMillis) {
      try {
        LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
        cachedBrokers = reader.getBrokerInfo();
        lastRefreshTimeMs = currTime;
      } catch (java.net.SocketTimeoutException e) {
        LOG.warn("Failed to update brokers", e);
      }
    }
  }

  @Override
  public GlobalPartitionInformation getBrokerForTopic(String topic) {
    refresh();
    for (GlobalPartitionInformation partitionInformation : cachedBrokers) {
      if (partitionInformation.getTopic().equals(topic)) {
        return partitionInformation;
      }
    }
    return null;
  }

  @Override
  public List<GlobalPartitionInformation> getAllBrokers() {
    refresh();
    return cachedBrokers;
  }

  @Override
  public void close() {
    reader.close();
  }
}
