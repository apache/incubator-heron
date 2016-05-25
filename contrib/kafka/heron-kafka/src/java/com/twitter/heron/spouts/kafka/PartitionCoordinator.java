package com.twitter.heron.spouts.kafka;

import com.twitter.heron.api.Config;
import com.twitter.heron.spouts.kafka.common.GlobalPartitionId;
import com.twitter.heron.spouts.kafka.common.GlobalPartitionInformation;
import com.twitter.heron.spouts.kafka.common.IOffsetStoreManager;
import com.twitter.heron.spouts.kafka.common.Partition;
import com.twitter.heron.storage.MetadataStore;
import com.twitter.heron.storage.StoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"rawtypes", "unchecked"})
public class PartitionCoordinator {

  public static final Logger LOG = LoggerFactory.getLogger(PartitionCoordinator.class);
  private final SpoutConfig spoutConfig;
  private final int taskIndex;
  private final int totalTasks;
  private final String topologyInstanceId;
  private final Map stormConf;
  private final int refreshFreqMSecs;

  private IOffsetStoreManager offsetStoreManager;

  private KafkaMetric.OffsetMetric kafkaOffsets;
  private DynamicKafkaBrokerReader dynamicBrokersReader;

  // Make it volatile since it would be accessed by multiple threads.
  // Notice: it is HashMap rather than any thread-safe map since every time when we update the Map,
  // we would replace it with a new reference rather than update an entry inside the map
  private volatile Map<GlobalPartitionId, PartitionManager> managers = new HashMap<>();
  private volatile Set<GlobalPartitionId> allPartitionIds = new TreeSet<>();

  private ScheduledExecutorService executor;
  private String componentId;

  /**
   * Creates a dynamic Zookeeper which reads data from zookeeper.
   */
  public PartitionCoordinator(
      Map stormConf,
      SpoutConfig spoutConfig,
      int taskIndex,
      int totalTasks,
      String topologyInstanceId,
      IOffsetStoreManager offsetStoreManager,
      KafkaMetric.OffsetMetric kafkaOffsetMetric,
      String componentId) {
    this.spoutConfig = spoutConfig;
    this.taskIndex = taskIndex;
    this.totalTasks = totalTasks;
    this.topologyInstanceId = topologyInstanceId;
    this.stormConf = stormConf;
    this.kafkaOffsets = kafkaOffsetMetric;
    this.offsetStoreManager = offsetStoreManager;
    this.componentId = componentId;

    this.refreshFreqMSecs = spoutConfig.refreshFreqMSecs;
    dynamicBrokersReader = new DynamicKafkaBrokerReader(spoutConfig.topic, spoutConfig.bootstrapBrokers);
    this.executor = Executors.newSingleThreadScheduledExecutor();
    scheduleConnectionRefresh();
    kafkaOffsetMetric.setCoordinator(this);
  }

  /**
   * Closes cnxn to zookeeper
   */
  protected void finalizer() {
    dynamicBrokersReader.close();
    offsetStoreManager.close();
  }

  /**
   * Caches parition manager and
   *
   * @return List of PartitionManager managed by this Coordinator.
   */
  public List<PartitionManager> getMyManagedPartitions() {
    return new ArrayList<>(managers.values());
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
   *
   * @param id Partition manager id
   * @return If this instance is managing id, it will return the corresponding PartitionManager.
   * Otherwise will return null.
   */
  public PartitionManager getManagerForId(GlobalPartitionId id) {
    return managers.get(id);
  }

  private Set<GlobalPartitionId> getAllPartitionsIdsFromBroker() throws Exception {
    GlobalPartitionInformation partitions = dynamicBrokersReader.getBrokerInfo();
    Set<GlobalPartitionId> partitionIds = new HashSet<>();
    for (Partition partition : partitions) {
      partitionIds.add(new GlobalPartitionId(partition.host.host, partition.host.port, partition.partition));
    }
    return partitionIds;
  }

  /**
   * Refreshes Partition Managers in case the brokers data changed in zookeeper
   */
  private void refresh() throws Exception {
    LOG.info("Refreshing partition manager: " + taskIndex + " & total tasks: " + totalTasks);
    Set<GlobalPartitionId> newAllPartitionIds = getAllPartitionsIdsFromBroker();
    LOG.info("All partitions: " + newAllPartitionIds);

    Map<GlobalPartitionId, PartitionManager> newManagers =
        new HashMap<>();
    int index = 0;
    for (GlobalPartitionId id : newAllPartitionIds) {
      if (index % totalTasks == taskIndex) {
        if (managers.containsKey(id)) {
          newManagers.put(id, managers.get(id));
          LOG.debug("Old partition manager continued: " + id);
        } else {
          LOG.info("Adding partition manager for " + id);
          MetadataStore offsetStore = offsetStoreManager.getStore(id);
          // We may consider calling "initialize" from within store managers
          offsetStore.initialize("offset_" + spoutConfig.id, stormConf.get(Config.TOPOLOGY_NAME).toString(),
              componentId,
              new StoreSerializer.DefaultSerializer<Map<Object, Object>>());
          PartitionManager partitionManager = new PartitionManager(
              topologyInstanceId, stormConf, spoutConfig, id, offsetStore, kafkaOffsets, componentId);
          newManagers.put(id, partitionManager);
          LOG.info("New partition manager added: " + id);
        }
      }
      index++;
    }

    // Close partition managers no longer needed.
    for (GlobalPartitionId id : managers.keySet()) {
      if (!newManagers.containsKey(id)) {
        managers.get(id).close();
        offsetStoreManager.close(managers.get(id).getStore());
        LOG.info("Unused partition manager closed: " + id);
      }
    }

    // Update the whole reference rather than entries
    allPartitionIds = newAllPartitionIds;
    managers = newManagers;
  }

  private void scheduleConnectionRefresh() {
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
