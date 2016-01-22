/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.spouts.kafka.SpoutConfig.ZkHosts;
import com.twitter.heron.storage.MetadataStore;

public class PartitionCoordinator {
  public static final Logger LOG = LoggerFactory.getLogger(PartitionCoordinator.class);
  private final SpoutConfig spoutConfig;
  private final int taskIndex;
  private final int totalTasks;
  private final String topologyInstanceId;
  private final Map stormConf;
  private final String zkPath;
  private final String topic;
  private final int refreshFreqMSecs;

  private MetadataStore storage;
  private KafkaMetric.OffsetMetric kafkaOffsets;
  private CuratorFramework curator;

  // Make it volatile since it would be accessed by multiple threads.
  // Notice: it is HashMap rather than any thread-safe map since every time when we update the Map,
  // we would replace it with a new reference rather than update an entry inside the map
  private volatile Map<GlobalPartitionId, PartitionManager> managers = new HashMap();
  private volatile Set<GlobalPartitionId> allPartitionIds = new TreeSet<GlobalPartitionId>();

  private ScheduledExecutorService executor;

  /** Creates a dynamic Zookeeper which reads data from zookeeper. */
  public PartitionCoordinator(
      Map stormConf,
      SpoutConfig spoutConfig,
      int taskIndex,
      int totalTasks,
      String topologyInstanceId,
      MetadataStore storage,
      KafkaMetric.OffsetMetric kafkaOffsetMetric) {
    this.spoutConfig = spoutConfig;
    this.taskIndex = taskIndex;
    this.totalTasks = totalTasks;
    this.topologyInstanceId = topologyInstanceId;
    this.stormConf = stormConf;
    this.kafkaOffsets = kafkaOffsetMetric;
    this.storage = storage;

    ZkHosts brokerConf = (ZkHosts) this.spoutConfig.hosts;
    this.refreshFreqMSecs = brokerConf.refreshFreqMSecs;
    this.zkPath = brokerConf.brokerZkPath;
    this.topic = spoutConfig.topic;
    this.executor = Executors.newSingleThreadScheduledExecutor();
    scheduleConnectionRefresh(brokerConf.brokerZkStr);
    kafkaOffsetMetric.setCoordinator(this);
  }

  /** Closes cnxn to zookeeper */
  protected void finalizer() {
    curator.close();
  }

  /**
   * Caches parition manager and
   * @return List of PartitionManager managed by this Coordinator.
   */
  public List<PartitionManager> getMyManagedPartitions() {
    return new ArrayList<PartitionManager>(managers.values());
  }

  /**
   * Returns the IDs of all partitions associated with the Kafka topic managed by this coordinator.
   *
   * @return The IDs of all partitions associated with the Kafka topic managed by this coordinator.
   */
  public Set<GlobalPartitionId> getAllPartitionIds() {
    return allPartitionIds;
  }

  /**
   * Returns partition manager for id if it is managed
   * @param id Partition manager id
   * @return If this instance is managing id, it will return the corresponding PartitionManager.
   * Otherwise will return null.
   */
  public PartitionManager getManagerForId(GlobalPartitionId id) {
    return managers.get(id);
  }

  private Set<GlobalPartitionId> getAllPartitionsIdsFromBroker() throws Exception {
    String topicBrokersPath = zkPath + "/topics/" + topic;
    String brokerInfoPath = zkPath + "/ids";
    Set<GlobalPartitionId> partitionIds = new TreeSet<GlobalPartitionId>();
    // TODO: remove curator with twitter Zk
    List<String> children = curator.getChildren().forPath(topicBrokersPath);
    for (String c: children) {
      String[] brokerData = readStringFromZk(brokerInfoPath + "/" + c).split(":");
      String host = brokerData[brokerData.length - 2];
      int port = Integer.parseInt(brokerData[brokerData.length - 1]);
      int numPartitions = Integer.parseInt(readStringFromZk(topicBrokersPath + "/" + c));
      for (int i = 0; i < numPartitions; ++i) {
        partitionIds.add(new GlobalPartitionId(host, port, i));
      }
    }
    return partitionIds;
  }

  private String readStringFromZk(String path) throws Exception {
    byte[] data =  curator.getData().forPath(path);
    if (data == null) {
      LOG.error("Read null data for path: " + path + " Task index: " + taskIndex);
    }
    return new String(data, "UTF-8");
  }

  /** Refreshes Partition Managers in case the brokers data changed in zookeeper */
  private void refresh() throws Exception {
    LOG.info("Refreshing partition manager: " + taskIndex + " & total tasks: " + totalTasks);
    Set<GlobalPartitionId> newAllPartitionIds = getAllPartitionsIdsFromBroker();
    LOG.debug("All partitions: " + newAllPartitionIds);

    Map<GlobalPartitionId, PartitionManager> newManagers =
        new HashMap<GlobalPartitionId, PartitionManager>();
    int index = 0;
    for (GlobalPartitionId id : newAllPartitionIds) {
      if (index % totalTasks == taskIndex) {
        if (managers.containsKey(id)) {
          newManagers.put(id, managers.get(id));
          LOG.debug("Old partition manager continued: " + id);
        } else {
          LOG.info("Adding partition manager for " + id);
          PartitionManager partitionManager = new PartitionManager(
              topologyInstanceId, stormConf, spoutConfig, id, storage, kafkaOffsets);
          newManagers.put(id, partitionManager);
          LOG.info("New partition manager added: " + id);
        }
      }
      index++;
    }

    // Close partition managers no longer needed.
    for (GlobalPartitionId id: managers.keySet()) {
      if (!newManagers.containsKey(id)) {
        managers.get(id).close();
        LOG.info("Unused partition manager closed: " + id);
      }
    }

    // Update the whole reference rather than entries
    allPartitionIds = newAllPartitionIds;
    managers = newManagers;
  }

  private void scheduleConnectionRefresh(String zkStr) {
    try {
      this.curator = CuratorFrameworkFactory.newClient(
          zkStr, spoutConfig.zookeeperStoreSessionTimeout, 15000,
          new RetryNTimes(spoutConfig.zookeeperRetryCount, spoutConfig.zookeeperRetryInterval));
      curator.start();
    } catch (IOException e) {
      LOG.info("Curator couldn't connect to Zk", e);
      throw new RuntimeException(e);
    }

    try {
      refresh();
    } catch (Exception e) {
      LOG.info("Malformed Zk Data for Rosette kafka", e);
      throw new RuntimeException(e);
    }

    Runnable refreshTask = new Runnable() {
      @Override
      public void run() {
        try {
          refresh();
        } catch (Exception e) {
          LOG.error("Failed to refresh partitions for: " + taskIndex, e);
        }
      }
    };

    this.executor.scheduleAtFixedRate(refreshTask, 0, refreshFreqMSecs, TimeUnit.MILLISECONDS);
  }
}
