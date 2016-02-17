package com.twitter.heron.spouts.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.metric.CombinedMetric;
import com.twitter.heron.api.metric.CountMetric;
import com.twitter.heron.api.metric.MeanReducer;
import com.twitter.heron.api.metric.ReducedMetric;
import com.twitter.heron.api.spout.ISpoutOutputCollector;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.storage.StormMetadataStore;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

// TODO: Add comments to describe the PartitionManager lifecycle
public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    public static class KafkaMessageId {
        public int partition;
        public long offset;

        public KafkaMessageId(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return String.format("{ \"partition\": %s, \"offset\": %s}", Integer.toString(partition), offset);
        }
    }

    // PartitionManager counters.
    private final CombinedMetric fetchAPILatencyMax;
    private final ReducedMetric fetchAPILatencyMean;
    private final CountMetric fetchAPICallCount;
    private final CountMetric fetchAPIMessageCount;
    private final CountMetric zeroEmits;
    private final CountMetric blobsFiltered;
    private final CountMetric tuplesFiltered;
    private final CountMetric tuplesAdded;
    private final CountMetric fetchExceptions;
    private final CountMetric tuplesInThePast;
    private final CountMetric deserializeEmpty;
    private final CountMetric deserializeNotEmpty;
    private final CountMetric deserializeException;
    private final CountMetric deserializeNullFiltered;
    private final CountMetric committedToStore;
    private final CountMetric nsInNext;
    private final CountMetric nsInFail;

    private final int partition;
    private final SpoutConfig spoutConfig;
    private final String topologyInstanceId;
    private final Map stormConf;
    private final StormMetadataStore storage;
    private final KafkaMetric.OffsetMetric kafkaOffsetMetric;

    private KafkaConsumer<byte[], byte[]> consumer;
    private AtomicLong currentKafkaOffset;
    private long committedTo;
    private final SortedMultiset<Long> pendingOffsets;
    private final LinkedList<Map.Entry<Long, List<Object>>> failedTuples;
    private long timestampOfLatestEmittedTuple;

    /**
     * Creates partition Manager
     */
    public PartitionManager(
            String topologyInstanceId,
            Map stormConf,
            SpoutConfig spoutConfig,
            Integer id,
            StormMetadataStore storage,
            KafkaMetric.OffsetMetric kafkaOffsetMetric,
            Properties kafkaProps) {
        this.partition = id;
        this.spoutConfig = spoutConfig;
        this.topologyInstanceId = topologyInstanceId;
        this.stormConf = stormConf;
        this.storage = storage;

        this.consumer = new KafkaConsumer<>(kafkaProps);
        consumer.assign(Collections.singletonList(new TopicPartition(spoutConfig.topic, partition)));

        /** Add counters */
        this.fetchAPILatencyMax = new CombinedMetric(new KafkaMetric.MaxMetric());
        this.fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        this.fetchAPICallCount = new CountMetric();
        this.fetchAPIMessageCount = new CountMetric();
        this.zeroEmits = new CountMetric();
        this.blobsFiltered = new CountMetric();
        this.tuplesFiltered = new CountMetric();
        this.tuplesAdded = new CountMetric();
        this.fetchExceptions = new CountMetric();
        this.tuplesInThePast = new CountMetric();
        this.deserializeEmpty = new CountMetric();
        this.deserializeNotEmpty = new CountMetric();
        this.deserializeException = new CountMetric();
        this.deserializeNullFiltered = new CountMetric();
        this.committedToStore = new CountMetric();
        this.nsInNext = new CountMetric();
        this.nsInFail = new CountMetric();

        this.committedTo = setCommitedToOffsetFromStore();
        this.currentKafkaOffset = new AtomicLong(committedTo);
        this.kafkaOffsetMetric = kafkaOffsetMetric;
        this.pendingOffsets = TreeMultiset.create();
        this.failedTuples = new LinkedList<>();
    }

    /**
     * Returns the ID of the Kafka partition managed by this manager.
     *
     * @return The ID of the Kafka partition managed by this manager.
     */
    public int getPartitionId() {
        return partition;
    }

    /**
     * Returns kafka offset where this PartitionManager is being read.
     */
    public long getCurrentKafkaOffset() {
        return currentKafkaOffset.get();
    }

    /**
     * Get committedTo offset value (for test).
     *
     * @return value of offset committed to primary store.
     */
    public long getCommitedTo() {
        return committedTo;
    }

    /**
     * Returns offset which have been processed successfully by the spout.
     */
    public long lastCompletedOffset() {
        // In fact this is not true, for the most cases it's actually currentKafkaOffset.get() - 1
        // Although, not changing for legacy reasons for now
        return currentKafkaOffset.get();
    }

    /**
     * If the {@code SpoutConfig} instance used for this partition manager specifies a timestamp
     * extractor, then this method will return the timestamp (in milliseconds) of the latest emitted
     * tuple stored in the Kafka partition associated with this manager. Otherwise, it will always
     * return 0.
     * <p>
     * Note that this is {@code max(timestamps_associated_with_emitted_tuples)} and might not
     * necessarily be the timestamp of the last emitted tuple, since the events stored in a Kafka
     * partition might be slightly out of order.
     * <p>
     * If there were no tuples in the partition last time we tried reading from it, this method will
     * return a special value of -1.
     *
     * @return The timestamp of the latest emitted tuple stored in this Kafka partition.
     */
    public long getTimestampOfLatestEmittedTuple() {
        return timestampOfLatestEmittedTuple;
    }

    /**
     * Returns the latest offset from which we should start reading this partition if we want to
     * capture all events that were published to this partition at time {@code timestamp}.
     * (visible for testing)
     *
     * NOTE: no such functionality in 0.9 consumer, left for legacy reasons
     *
     * @param timestamp The timestamp at which the events were added to this partition.
     * @return The offset at which events were published at time {@code timestamp} (or later).
     */
    @Deprecated
    public long getOffsetForTimestamp(long timestamp) {
        return 0L;
    }


    /**
     * Add metrics data to monitor health of spout
     */
    public Map getMetricsDataMap() {
        // These metrics are done as map because PartitionManager is created outside of
        // open method.
        Map ret = new HashMap();
        ret.put(partition + "/fetchAPILatencyMax", fetchAPILatencyMax.getValueAndReset());
        ret.put(partition + "/fetchAPILatencyMean", fetchAPILatencyMean.getValueAndReset());
        ret.put(partition + "/fetchAPICallCount", fetchAPICallCount.getValueAndReset());
        ret.put(partition + "/fetchAPIMessageCount", fetchAPIMessageCount.getValueAndReset());
        ret.put(partition + "/fetchException", fetchExceptions.getValueAndReset());
        ret.put(partition + "/zeroEmits", zeroEmits.getValueAndReset());
        ret.put(partition + "/blobsFiltered", blobsFiltered.getValueAndReset());
        ret.put(partition + "/tuplesFiltered", tuplesFiltered.getValueAndReset());
        ret.put(partition + "/tuplesAdded", tuplesAdded.getValueAndReset());
        ret.put(partition + "/tuplesInThePast", tuplesInThePast.getValueAndReset());
        ret.put(partition + "/deserializeEmpty", deserializeEmpty.getValueAndReset());
        ret.put(partition + "/deserializeNotEmpty", deserializeNotEmpty.getValueAndReset());
        ret.put(partition + "/deserializeException", deserializeException.getValueAndReset());
        ret.put(partition + "/deserializeNullFiltered", deserializeNullFiltered.getValueAndReset());
        ret.put(partition + "/committedToStore", committedToStore.getValueAndReset());
        ret.put(partition + "/nsInNext", nsInNext.getValueAndReset());
        return ret;
    }

    /**
     * Fetches new data from Kafka partition. Returns the list of kafka message offsets emitted.
     */
    public ArrayList<KafkaMessageId> next(ISpoutOutputCollector collector) {
        long startTime = System.nanoTime();
        ArrayList<KafkaMessageId> emittedIds = emitFailedTuples(collector);
        // Emit messages from kafka
        ConsumerRecords<byte[], byte[]> waitingToEmit = fetchMessageFromKafka(currentKafkaOffset.get());

        if (!waitingToEmit.iterator().hasNext()) {
            zeroEmits.incr();
            return emittedIds;
        }
        for (ConsumerRecord<byte[], byte[]> consumerRecord : waitingToEmit) {
            long offset = consumerRecord.offset();
            if (!applyPayloadFilter(consumerRecord.value())) {
                Iterable<List<Object>> tuples = deserializeRecord(consumerRecord);
                if (tuples != null) {
                    for (List<Object> tuple : tuples) {
                        updateLatestEmittedTupleTimestamp(tuple);
                        if (!applyTupleFilter(tuple)) {
                            // The anchor holds the offset for this message to guarantee that
                            // this tuple gets replayed in case of failure. msg.offset(), is the offset of next
                            // message in kafka.
                            emittedIds.add(emitMessage(tuple, currentKafkaOffset.get(), collector));
                        }
                    }
                }
            }
            currentKafkaOffset.set(offset + 1);  // Move current offset to point to this message.
        }
        long endTime = System.nanoTime();
        nsInNext.incrBy(endTime - startTime);
        return emittedIds;
    }

    /**
     * Production has been processed by all the downstream nodes. Remove it from buffer.
     */
    public void ack(Long offset) {
        synchronized (pendingOffsets) {
            pendingOffsets.remove(offset);
        }
    }

    /**
     * Failed to process the data. Replay from the offset which failed
     */
    public void fail(Long offset) {
        long startTime = System.nanoTime();
        synchronized (pendingOffsets) {
            pendingOffsets.remove(offset);
        }
        if (spoutConfig.shouldReplay) {
            currentKafkaOffset.set(offset);
        } else if (spoutConfig.maxFailedTupleCount > 0
                && failedTuples.size() < spoutConfig.maxFailedTupleCount) {
            try {
                ConsumerRecord<byte[], byte[]> failedMessage = readMessageAtOffset(offset);
                if (failedMessage != null) {
                    Iterable<List<Object>> tuples =
                            deserializeRecord(failedMessage);
                    // Add failed message to emit
                    if (tuples != null) {
                        synchronized (failedTuples) {
                            for (List<Object> tuple : tuples) {
                                failedTuples.addLast(
                                        new AbstractMap.SimpleEntry<Long, List<Object>>(offset, tuple));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Caught exception when trying to read from kafka ", e);
                fetchExceptions.incr();
            }
        }
        long endTime = System.nanoTime();
        nsInFail.incrBy(endTime - startTime);
    }

    /**
     * Commits offset read so far for this partition to a stormstore.
     * The spout stores the state of the offsets it has consumed in a persistent store.
     * Offsets for partitions will be stored in these datastore, if "0", "1" are
     * ids for the partitions:
     * |  0         |     1
     * {id} |  offset1   |     offset2
     */
    public void commit(KafkaConsumer offsetCommitConsumer) {
        long committedOffset;
        synchronized (pendingOffsets) {
            committedOffset = pendingOffsets.isEmpty()
                    ? currentKafkaOffset.get()
                    : pendingOffsets.firstEntry().getElement();
        }
        if (committedOffset != committedTo) {
            if (storage != null) {
                ImmutableMap data = ImmutableMap.builder()
                        .put("topic", spoutConfig.topic)
                        .put("topology", ImmutableMap.of("id", topologyInstanceId,
                                "name", stormConf.get(Config.TOPOLOGY_NAME)))
                        .put("offset", committedOffset).build();
                storage.setPKey(Integer.toString(partition), data);
            } else {
                Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                commitData.put(new TopicPartition(spoutConfig.topic, partition), new OffsetAndMetadata(committedOffset));
                offsetCommitConsumer.commitSync(commitData);
            }
            committedTo = committedOffset;
            committedToStore.incr();
        }
    }

    /**
     * Get consumer
     */
    public KafkaConsumer<byte[], byte[]> getConsumer() {
        return consumer;
    }

    public void close() {
        consumer.close();
    }

    /**
     * Handles reemitting failed tuples
     *
     * @param collector Emits to this collector.
     * @return List of message ID for emitted tuple.
     */
    private ArrayList<KafkaMessageId> emitFailedTuples(ISpoutOutputCollector collector) {
        ArrayList<KafkaMessageId> emittedIds = new ArrayList<>();

        // Emit failed tuples. Note that each call to next will try to emit from
        // failed tuple buffer as well as from kafka.
        LinkedList<Map.Entry<Long, List<Object>>> copyTuples = new LinkedList<>();
        synchronized (failedTuples) {
            copyTuples = new LinkedList<>(failedTuples);
            failedTuples.clear();
        }
        Iterator<Map.Entry<Long, List<Object>>> iter = copyTuples.iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, List<Object>> tupleAndOffset = iter.next();
            emittedIds.add(emitMessage(tupleAndOffset.getValue(), tupleAndOffset.getKey(), collector));
        }
        return emittedIds;
    }

    /**
     * Filter operator before deserialize.
     *
     * @param payload Data received from kafka
     * @return true if filtered.
     */
    private boolean applyPayloadFilter(byte[] payload) {
        if (spoutConfig.filterOperator != null) {
            // Run lag filter
            if (spoutConfig.filterOperator.filter(
                    payload, kafkaOffsetMetric.getLastCalculatedTotalSpoutLag())) {
                blobsFiltered.incr();
                return true;
            }
        }
        return false;
    }

    /**
     * Filter operator after deserialize.
     *
     * @param tuple Deserialized tuple.
     * @return true if filtered.
     */
    private boolean applyTupleFilter(List<Object> tuple) {
        if (spoutConfig.filterOperator != null) {
            if (spoutConfig.filterOperator.filter(
                    tuple, kafkaOffsetMetric.getLastCalculatedTotalSpoutLag())) {
                tuplesFiltered.incr();
                return true;
            }
        }
        return false;
    }

    /**
     * Adds messages from kafka which were failed to a failed buffer. Will be used to not replay
     * all messages from failed offset
     *
     * @param offset
     */
    private ConsumerRecord<byte[], byte[]> readMessageAtOffset(Long offset) {
        ConsumerRecords<byte[], byte[]> msgs = fetchMessageFromKafka(offset);
        Long currOffset = offset;
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
            // Return the first item such that
            if (msg.offset() > currOffset) {
                return msg;
            }
        }
        return null;
    }

    /**
     * Emits tuple and update offsets.
     *
     * @param tuple     Tuple to be emitted.
     * @param collector Output collector.
     * @param offset    Value of offset where message is emitted.
     * @return anchor containing offset and partition of the tuple.
     */
    private KafkaMessageId emitMessage(
            List<Object> tuple, Long offset, ISpoutOutputCollector collector) {
        // The anchor holds the offset for this message to guarantee that
        // this tuple gets replayed in case of failure. msg.offset(), is the offset of next
        // message in kafka.
        KafkaMessageId msgId = new KafkaMessageId(partition, offset);
        synchronized (pendingOffsets) {
            pendingOffsets.add(offset);
        }
        if (spoutConfig.shouldAnchorEmits) {
            collector.emit(Utils.DEFAULT_STREAM_ID, tuple, msgId);
        } else {
            collector.emit(Utils.DEFAULT_STREAM_ID, tuple, null);
        }
        tuplesAdded.incr();
        return msgId;
    }

    /**
     * Deserialize tuples and add counters for various deserialization scenarios
     */
    private Iterable<List<Object>> deserializeRecord(ConsumerRecord<byte[], byte[]> record) {
        Iterable<List<Object>> tuples;
        try {
            tuples = generateTuples(record);
            if (tuples != null && tuples.iterator().hasNext()) {
                deserializeNotEmpty.incr();
            } else {
                deserializeEmpty.incr();
            }
            if (tuples == null) {
                deserializeNullFiltered.incr();
            }
            return tuples;
        } catch (Exception e) {
            deserializeException.incr();
            LOG.error("Exception deserialize message: " + byteArrayToHex(record.value())
                    + " at offset " + record.offset() + " for kafka partition " + partition, e);
            throw new RuntimeException("Failure to deserialize byte array " + e.getMessage());
        }
    }

    private Iterable<List<Object>> generateTuples(ConsumerRecord<byte[], byte[]> record) {
        Iterable<List<Object>> tups;
        if (record.value() == null || record.value().length == 0) {
            return null;
        }
        if (record.key() != null && record.key().length > 0 && spoutConfig.scheme instanceof KeyValueSchemeAsMultiScheme) {
            tups = ((KeyValueSchemeAsMultiScheme) spoutConfig.scheme).deserializeKeyAndValue(record.key(), record.value());
        } else {
            if (spoutConfig.scheme instanceof StringMultiSchemeWithTopic) {
                tups = ((StringMultiSchemeWithTopic)spoutConfig.scheme).deserializeWithTopic(spoutConfig.topic, record.value());
            } else {
                tups = spoutConfig.scheme.deserialize(record.value());
            }
        }
        return tups;
    }

    /**
     * Return the offset to start kafka spout from by looking at commits stored in MH and spoutConfig.
     *
     * @return Offset to start partition from.
     */
    private long setCommitedToOffsetFromStore() {
        long offset;
        if (storage != null) {
            offset = restoreOffsetFromStorage();
        } else {
            offset = restoreOffsetFromKafka();
        }

        LOG.info("Starting reading from Kafka partition " + partition + " at offset " + offset);
        return offset;
    }

    private long restoreOffsetFromKafka() {
        long offsetToReturn;
        OffsetAndMetadata lastCommitted = consumer.committed(new TopicPartition(spoutConfig.topic, partition));
        if (lastCommitted != null) {
            offsetToReturn = checkRecoveredOffset(lastCommitted.offset());
        } else {
            offsetToReturn = getLimitOffset(spoutConfig.startFromLatestOffset);
        }

        return offsetToReturn;
    }

    private long restoreOffsetFromStorage() {
        String topologyId = null;
        long offset = 0;
        long offsetToReturn = 0;

        // Try to read the offset value for previous run of topology.
        try {

            Map<Object, Object> json = readStore();
            if (json != null) {
                topologyId = ((Map<Object, Object>) json.get("topology")).get("id").toString();
                offset = Long.parseLong(json.get("offset").toString());
                LOG.info("Found older value topology = " + topologyId + " offset = " + offset);
            }
        } catch (Exception e) {
            topologyId = null;
            LOG.error("Error reading and/or parsing commit offset", e);
        }

        if (topologyId == null) {
            // The topology is submitted for first time (no data in commit store).
            offsetToReturn = getLimitOffset(spoutConfig.startFromLatestOffset);
            LOG.info("No topology. Setting offset to the value defined in the spout config.");
        } else if (!topologyInstanceId.equals(topologyId) && spoutConfig.forceFromStart) {
            // The topology is re-submitted and spoutConfig.forceStartOffsetTime is called.
            offsetToReturn = getLimitOffset(spoutConfig.startFromLatestOffset);
            LOG.info("Topology not matched. Setting offset to the value defined in the spout config.");
        } else {
            offsetToReturn = checkRecoveredOffset(offset);
        }
        return offsetToReturn;
    }

    /**
     * @return latest or earliest offset
     */
    private long getLimitOffset(boolean latest) {
        TopicPartition tp = new TopicPartition(spoutConfig.topic, partition);
        if (latest) {
            consumer.seekToEnd(tp);
        } else {
            consumer.seekToBeginning(tp);
        }

        return consumer.position(tp);
    }

    private long checkRecoveredOffset(long offset) {
        long offsetToReturn;

        // Default case: spoutConfig.forceStartOffsetTime is not called, or Kafka spout instance was
        // restarted due to exception or crashes.
        // In this case topologyId stored in commitStore will match topologyInstanceId.
        long earliestOffset = getLimitOffset(false);
        if (earliestOffset > offset) {
            // Can't continue from offset stored in stores. Try to get best possible offset.
            offsetToReturn = getLimitOffset(spoutConfig.startFromLatestOffset);
            LOG.info("Stored offset (" + offset + ") is older than the earliest available offset ("
                    + earliestOffset + "). Using offset " + offsetToReturn);
        } else {
            offsetToReturn = offset;
            LOG.info("Using the offset read from the commit store: " + offsetToReturn);
        }

        // Verify that new offset doesn't throw OffsetOutOfRangeException.(This can happen in
        // certain scenario when kafka offset for a partition is reset to zero).
        try {
            fetchMessageFromKafka(offset);
        } catch (Exception e) {
            // Kafka partition offset has reset back to start from 0. (This may happen in rare cases)
            // Note: This may happen without OffsetOutOfRange. But consumers can't distinguish this
            // scenario.
            // We will move to earliest offset in this case.
            LOG.error("Kafka broker offset data may have been corrupted. Starting from latest");
            offsetToReturn = getLimitOffset(true);
        }

        return offsetToReturn;
    }

    /**
     * Updates the timestamp of the latest emitted tuple, if the spout config specifies a timestamp
     * extractor.
     *
     * @param tuple The last emitted tuple.
     */
    private void updateLatestEmittedTupleTimestamp(Object tuple) {
        if (spoutConfig.timestampExtractor != null) {
            long timestamp = spoutConfig.timestampExtractor.getTimestamp(tuple);
            if (timestamp > timestampOfLatestEmittedTuple) {
                timestampOfLatestEmittedTuple = timestamp;
            } else if (timestamp < timestampOfLatestEmittedTuple - spoutConfig.tupleInThePastThreshold) {
                tuplesInThePast.incr();
            }
        }
    }

    private String byteArrayToHex(byte[] payload) {
        StringBuilder sb = new StringBuilder();
        if (payload == null) {
            return "NULL";
        }
        for (byte b : payload) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    private Map<Object, Object> readStore() {
        try {
            byte[] data = storage.readPKey(Integer.toString(partition));
            if (data != null && data.length > 0) {
                return (Map<Object, Object>) JSONValue.parse(new String(data, "UTF-8"));
            }
        } catch (Exception e) {
            LOG.error("Failed to read state in primary store for " + partition);
        }
        return null;
    }

    /**
     * Read data from kafka. In case kafka is not reachable, empty message list will be returned.
     * Constant function, not supposed to update any state of PartitionManager (like
     * currentKafkaOffset)
     */
    private ConsumerRecords<byte[], byte[]> fetchMessageFromKafka(Long fromOffset) {
        ConsumerRecords<byte[], byte[]> msgs = new ConsumerRecords<>(Collections.EMPTY_MAP);

        // TODO: not sure if this can be done with new consumer, leaving commented for now
        // Implemented to support minimum lag. Some topology need to run kafka spout at fixed lag.
        /*if (spoutConfig.fixedLagBytes > 0) {
            long latestOffset = getOffsetForTimestamp(OffsetRequest.LatestTime());
            long lag = latestOffset - fromOffset;
            if (lag <= spoutConfig.fixedLagBytes) {
                return msgs;
            }
        }*/

        long start = System.nanoTime();
        // currentKafkaOffset will be update in next method which iterates on returned messages.
        consumer.seek(new TopicPartition(spoutConfig.topic, partition), fromOffset);
        try {
            synchronized (PartitionManager.class) {
                // Simple consumer can't request fetch in parallel.
                msgs = consumer.poll(1000);
            }
        } catch (OffsetOutOfRangeException e) {
            // Restart
            throw new RuntimeException("Restart to recover from offset out of range exception");
        } catch (Exception e) {
            LOG.error("Caught exception when trying to read from kafka ", e);
            fetchExceptions.incr();
        }
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        int numMessages = msgs.count();
        fetchAPILatencyMax.update(millis);
        fetchAPILatencyMean.update(millis);
        fetchAPICallCount.incr();
        fetchAPIMessageCount.incrBy(numMessages);

        if (numMessages == 0) {
            timestampOfLatestEmittedTuple = -1L;
        } else {
            LOG.debug("Non-empty fetch from Kafka, current consumer position for partition " + partition + ":" +
                    consumer.position(new TopicPartition(spoutConfig.topic, partition)));
       }

        return msgs;
    }

    /**
     * Unit Test helper Functions
     */
    public void setConsumer(KafkaConsumer<byte[], byte[]> simpleConsumer) {
        this.consumer = simpleConsumer;
    }

    public SortedMultiset<Long> getPendingOffsets() {
        return pendingOffsets;
    }
}
