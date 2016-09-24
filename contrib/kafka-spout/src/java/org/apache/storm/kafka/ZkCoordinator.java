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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.kafka.KafkaUtils.taskId;

public class ZkCoordinator implements PartitionCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

  private SpoutConfig spoutConfig;
  private int taskIndex;
  private int totalTasks;
  private String topologyInstanceId;
  private Map<Partition, PartitionManager> managers = new HashMap<>();
  private List<PartitionManager> cachedList = new ArrayList<PartitionManager>();
  private Long lastRefreshTime = null;
  private int refreshFreqMs;
  private DynamicPartitionConnections connections;
  private  DynamicBrokersReader reader;
  private ZkState state;
  private Map<String, Object> stormConf;

  public ZkCoordinator(DynamicPartitionConnections connections,
                       Map<String, Object> stormConf,
                       SpoutConfig spoutConfig,
                       ZkState state,
                       int taskIndex,
                       int totalTasks,
                       String topologyInstanceId) {
    this(connections, stormConf, spoutConfig, state, taskIndex,
        totalTasks, topologyInstanceId, buildReader(stormConf, spoutConfig));
  }

  public ZkCoordinator(DynamicPartitionConnections connections,
                       Map<String, Object> stormConf,
                       SpoutConfig spoutConfig,
                       ZkState state,
                       int taskIndex,
                       int totalTasks,
                       String topologyInstanceId,
                       DynamicBrokersReader reader) {
    this.spoutConfig = spoutConfig;
    this.connections = connections;
    this.taskIndex = taskIndex;
    this.totalTasks = totalTasks;
    this.topologyInstanceId = topologyInstanceId;
    this.stormConf = stormConf;
    this.state = state;
    ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
    this.refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
    this.reader = reader;
  }

  public int getTaskIndex() {
    return this.taskIndex;
  }

  private static DynamicBrokersReader buildReader(Map<String, Object> stormConf,
                                                  SpoutConfig spoutConfig) {
    ZkHosts hosts = (ZkHosts) spoutConfig.hosts;
    return new DynamicBrokersReader(stormConf, hosts.brokerZkStr,
        hosts.brokerZkPath, spoutConfig.topic);
  }

  @Override
  public List<PartitionManager> getMyManagedPartitions() {
    if (lastRefreshTime == null
        || (System.currentTimeMillis() - lastRefreshTime) > refreshFreqMs) {
      refresh();
      lastRefreshTime = System.currentTimeMillis();
    }
    return cachedList;
  }

  @Override
  public void refresh() {
    try {
      LOG.info(taskId(taskIndex, totalTasks) + "Refreshing partition manager connections");
      List<GlobalPartitionInformation> brokerInfo = reader.getBrokerInfo();
      List<Partition> mine =
          KafkaUtils.calculatePartitionsForTask(brokerInfo, totalTasks, taskIndex);

      Set<Partition> curr = managers.keySet();
      Set<Partition> newPartitions = new HashSet<Partition>(mine);
      newPartitions.removeAll(curr);

      Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
      deletedPartitions.removeAll(mine);

      LOG.info(taskId(taskIndex, totalTasks)
          + "Deleted partition managers: " + deletedPartitions.toString());

      for (Partition id : deletedPartitions) {
        PartitionManager man = managers.remove(id);
        man.close();
      }
      LOG.info(taskId(taskIndex, totalTasks)
          + "New partition managers: " + newPartitions.toString());

      for (Partition id : newPartitions) {
        PartitionManager man = new PartitionManager(connections, topologyInstanceId, state,
            stormConf, spoutConfig, id);
        managers.put(id, man);
      }
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    cachedList = new ArrayList<PartitionManager>(managers.values());
    LOG.info(taskId(taskIndex, totalTasks) + "Finished refreshing");
  }

  @Override
  public PartitionManager getManager(Partition partition) {
    return managers.get(partition);
  }
}
