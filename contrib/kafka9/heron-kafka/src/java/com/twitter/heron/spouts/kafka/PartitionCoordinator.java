package com.twitter.heron.spouts.kafka;

import com.twitter.heron.api.Config;
import com.twitter.heron.storage.StormMetadataStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionCoordinator {

    public static final Logger LOG = LoggerFactory.getLogger(PartitionCoordinator.class);
    private final SpoutConfig spoutConfig;
    private final int taskIndex;
    private final int totalTasks;
    private final String topologyInstanceId;
    private final Map stormConf;
    private final String topic;
    private final int refreshFreqMSecs;

    private StormMetadataStore storage;
    private KafkaMetric.OffsetMetric kafkaOffsets;
    private DynamicKafkaBrokerReader dynamicBrokersReader;

    // Make it volatile since it would be accessed by multiple threads.
    // Notice: it is HashMap rather than any thread-safe map since every time when we update the Map,
    // we would replace it with a new reference rather than update an entry inside the map
    private volatile Map<Integer, PartitionManager> managers = new HashMap<>();
    private volatile Set<Integer> allPartitionIds = new TreeSet<Integer>();

    private ScheduledExecutorService executor;

    private Properties kafkaProps;
    public final KafkaConsumer<byte[], byte[]> commitOffsetConsumer;

    /** Creates a dynamic Zookeeper which reads data from zookeeper. */
    public PartitionCoordinator(
            Map stormConf,
            SpoutConfig spoutConfig,
            int taskIndex,
            int totalTasks,
            String topologyInstanceId,
            StormMetadataStore storage,
            KafkaMetric.OffsetMetric kafkaOffsetMetric,
            String componentId) {
        this.spoutConfig = spoutConfig;
        this.taskIndex = taskIndex;
        this.totalTasks = totalTasks;
        this.topologyInstanceId = topologyInstanceId;
        this.stormConf = stormConf;
        this.kafkaOffsets = kafkaOffsetMetric;
        this.storage = storage;

        this.kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", spoutConfig.bootstrapBrokers);
        kafkaProps.put("group.id", composeConsumerGroupId(componentId));
        kafkaProps.put("enable.auto.commit", "false");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.commitOffsetConsumer = new KafkaConsumer<>(kafkaProps);

        this.refreshFreqMSecs = spoutConfig.refreshFreqMSecs;
        this.topic = spoutConfig.topic;
        dynamicBrokersReader = new DynamicKafkaBrokerReader(topic, spoutConfig.bootstrapBrokers);
        this.executor = Executors.newSingleThreadScheduledExecutor();
        scheduleConnectionRefresh();
        kafkaOffsetMetric.setCoordinator(this);
    }

    private String composeConsumerGroupId(String componentId) {
        String consumerGroupId = String.format("%s_%s_%s", stormConf.get(Config.TOPOLOGY_NAME).toString(), spoutConfig.id, componentId);
        if (spoutConfig.storeOffsetsPerTopologyInstance) {
            consumerGroupId = consumerGroupId + "_" + topologyInstanceId;
        }

        return consumerGroupId;
    }

    /** Closes cnxn to zookeeper */
    protected void finalizer() {
        dynamicBrokersReader.close();
    }

    /**
     * Caches parition manager and
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
    public Set<Integer> getAllPartitionIds() {
        return allPartitionIds;
    }

    /**
     * Returns partition manager for id if it is managed
     * @param id Partition manager id
     * @return If this instance is managing id, it will return the corresponding PartitionManager.
     * Otherwise will return null.
     */
    public PartitionManager getManagerForId(int id) {
        return managers.get(id);
    }

    private Set<Integer> getAllPartitionsIdsFromBroker() throws Exception {
        GlobalPartitionInformation partitions = dynamicBrokersReader.getBrokerInfo();
        Set<Integer> partitionIds = new HashSet<>();
        for (Partition partition : partitions) {
            partitionIds.add(partition.partition);
        }
        return partitionIds;
    }

    /** Refreshes Partition Managers in case the brokers data changed in zookeeper */
    private void refresh() throws Exception {
        LOG.info("Refreshing partition manager: " + taskIndex + " & total tasks: " + totalTasks);
        Set<Integer> newAllPartitionIds = getAllPartitionsIdsFromBroker();
        LOG.info("All partitions: " + newAllPartitionIds);

        Map<Integer, PartitionManager> newManagers =
                new HashMap<>();
        int index = 0;
        for (Integer id : newAllPartitionIds) {
            if (index % totalTasks == taskIndex) {
                if (managers.containsKey(id)) {
                    newManagers.put(id, managers.get(id));
                    LOG.debug("Old partition manager continued: " + id);
                } else {
                    LOG.info("Adding partition manager for " + id);
                    PartitionManager partitionManager = new PartitionManager(
                            topologyInstanceId, stormConf, spoutConfig, id, storage, kafkaOffsets, kafkaProps);
                    newManagers.put(id, partitionManager);
                    LOG.info("New partition manager added: " + id);
                }
            }
            index++;
        }

        // Close partition managers no longer needed.
        for (Integer id: managers.keySet()) {
            if (!newManagers.containsKey(id)) {
                managers.get(id).close();
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
