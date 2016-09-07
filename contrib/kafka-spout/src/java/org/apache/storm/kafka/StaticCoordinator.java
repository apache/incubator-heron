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
import java.util.List;
import java.util.Map;

import org.apache.storm.kafka.trident.GlobalPartitionInformation;

public class StaticCoordinator implements PartitionCoordinator {
  private Map<Partition, PartitionManager> managers = new HashMap<>();
  private List<PartitionManager> allManagers = new ArrayList<>();

  public StaticCoordinator(DynamicPartitionConnections connections,
                           Map<String, Object> stormConf,
                           SpoutConfig config,
                           ZkState state,
                           int taskIndex,
                           int totalTasks,
                           String topologyInstanceId) {
    StaticHosts hosts = (StaticHosts) config.hosts;
    List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();
    partitions.add(hosts.getPartitionInformation());
    List<Partition> myPartitions =
        KafkaUtils.calculatePartitionsForTask(partitions, totalTasks, taskIndex);

    for (Partition myPartition : myPartitions) {
      managers.put(myPartition,
          new PartitionManager(connections, topologyInstanceId,
              state, stormConf, config, myPartition));
    }
    allManagers = new ArrayList<>(managers.values());
  }

  @Override
  public List<PartitionManager> getMyManagedPartitions() {
    return allManagers;
  }

  public PartitionManager getManager(Partition partition) {
    return managers.get(partition);
  }

  @Override
  public void refresh() {
    return;
  }

}
