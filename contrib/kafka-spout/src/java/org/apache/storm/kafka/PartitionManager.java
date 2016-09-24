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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.storm.Config;
import org.apache.storm.kafka.KafkaSpout.EmitState;
import org.apache.storm.kafka.trident.MaxMetric;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class PartitionManager {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

  private final CombinedMetric fetchAPILatencyMax;
  private final ReducedMetric fetchAPILatencyMean;
  private final CountMetric fetchAPICallCount;
  private final CountMetric fetchAPIMessageCount;
  // Count of messages which could not be emitted or retried because they were deleted from kafka
  private final CountMetric lostMessageCount;
  // Count of messages which were not retried because failedMsgRetryManager didn't consider offset
  // eligible for retry
  private final CountMetric messageIneligibleForRetryCount;
  private Long emittedToOffset;
  // pending key = Kafka offset, value = time at which the message was first submitted to the
  // topology
  private SortedMap<Long, Long> pending = new TreeMap<Long, Long>();
  private final FailedMsgRetryManager failedMsgRetryManager;

  // retryRecords key = Kafka offset, value = retry info for the given message
  private Long committedTo;
  private LinkedList<MessageAndOffset> waitingToEmit = new LinkedList<MessageAndOffset>();
  private Partition partition;
  private SpoutConfig spoutConfig;
  private String topologyInstanceId;
  private SimpleConsumer consumer;
  private DynamicPartitionConnections connections;
  private ZkState state;
  private Map<String, Object> stormConf;
  private long numberFailed;
  private long numberAcked;

  /***
   * Constructor
   * @param connections
   * @param topologyInstanceId
   * @param state
   * @param stormConf
   * @param spoutConfig
   * @param id
   */
  @SuppressWarnings("unchecked")
  public PartitionManager(DynamicPartitionConnections connections,
                          String topologyInstanceId,
                          ZkState state,
                          Map<String, Object> stormConf,
                          SpoutConfig spoutConfig,
                          Partition id) {
    this.partition = id;
    this.connections = connections;
    this.spoutConfig = spoutConfig;
    this.topologyInstanceId = topologyInstanceId;
    this.consumer = connections.register(id.host, id.topic, id.partition);
    this.state = state;
    this.stormConf = stormConf;
    this.numberAcked = this.numberFailed = 0;

    try {
      failedMsgRetryManager = (FailedMsgRetryManager)
          Class.forName(spoutConfig.failedMsgRetryManagerClass)
              .newInstance();
      failedMsgRetryManager.prepare(spoutConfig, stormConf);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(
          String.format("Failed to create an instance of <%s> from: <%s>",
              FailedMsgRetryManager.class,
              spoutConfig.failedMsgRetryManagerClass), e);
    }

    String jsonTopologyId = null;
    Long jsonOffset = null;
    String path = committedPath();
    try {
      Map<Object, Object> json = state.readJSON(path);
      LOG.info("Read partition information from: " + path + "  --> " + json);
      if (json != null) {
        jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
        jsonOffset = (Long) json.get("offset");
      }
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Throwable e) {
      LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
    }

    String topic = partition.topic;
    Long currentOffset = KafkaUtils.getOffset(consumer, topic, id.partition, spoutConfig);

    if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
      committedTo = currentOffset;
      LOG.info("No partition information found, using configuration to determine offset");
    } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.ignoreZkOffsets) {
      committedTo =
          KafkaUtils.getOffset(consumer, topic, id.partition, spoutConfig.startOffsetTime);
      LOG.info("Topology change detected and ignore zookeeper offsets set to true, "
          + "using configuration to determine offset");
    } else {
      committedTo = jsonOffset;
      LOG.info("Read last commit offset from zookeeper: " + committedTo
          + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId);
    }

    if (currentOffset - committedTo > spoutConfig.maxOffsetBehind || committedTo <= 0) {
      LOG.info("Last commit offset from zookeeper: " + committedTo);
      Long lastCommittedOffset = committedTo;
      committedTo = currentOffset;
      LOG.info("Commit offset " + lastCommittedOffset + " is more than "
          + spoutConfig.maxOffsetBehind + " behind latest offset " + currentOffset
          + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
    }

    LOG.info("Starting Kafka " + consumer.host() + " " + id + " from offset " + committedTo);
    emittedToOffset = committedTo;

    fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
    fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
    fetchAPICallCount = new CountMetric();
    fetchAPIMessageCount = new CountMetric();
    lostMessageCount = new CountMetric();
    messageIneligibleForRetryCount = new CountMetric();
  }

  public Map<String, Object> getMetricsDataMap() {
    Map<String, Object> ret = new HashMap<>();
    ret.put(partition + "/fetchAPILatencyMax", fetchAPILatencyMax.getValueAndReset());
    ret.put(partition + "/fetchAPILatencyMean", fetchAPILatencyMean.getValueAndReset());
    ret.put(partition + "/fetchAPICallCount", fetchAPICallCount.getValueAndReset());
    ret.put(partition + "/fetchAPIMessageCount", fetchAPIMessageCount.getValueAndReset());
    ret.put(partition + "/lostMessageCount", lostMessageCount.getValueAndReset());
    ret.put(partition + "/messageIneligibleForRetryCount",
        messageIneligibleForRetryCount.getValueAndReset());
    return ret;
  }

  //returns false if it's reached the end of current batch
  public EmitState next(SpoutOutputCollector collector) {
    if (waitingToEmit.isEmpty()) {
      fill();
    }
    while (true) {
      MessageAndOffset toEmit = waitingToEmit.pollFirst();
      if (toEmit == null) {
        return EmitState.NO_EMITTED;
      }

      Iterable<List<Object>> tups;
      if (spoutConfig.scheme instanceof MessageMetadataSchemeAsMultiScheme) {
        tups = KafkaUtils.generateTuples((MessageMetadataSchemeAsMultiScheme) spoutConfig.scheme,
            toEmit.message(),
            partition,
            toEmit.offset());
      } else {
        tups = KafkaUtils.generateTuples(spoutConfig, toEmit.message(), partition.topic);
      }

      if ((tups != null) && tups.iterator().hasNext()) {
        if (!Strings.isNullOrEmpty(spoutConfig.outputStreamId)) {
          for (List<Object> tup : tups) {
            collector.emit(spoutConfig.topic,
                tup, new KafkaMessageId(partition, toEmit.offset()));
          }
        } else {
          for (List<Object> tup : tups) {
            collector.emit(tup, new KafkaMessageId(partition, toEmit.offset()));
          }
        }
        break;
      } else {
        ack(toEmit.offset());
      }
    }
    if (!waitingToEmit.isEmpty()) {
      return EmitState.EMITTED_MORE_LEFT;
    } else {
      return EmitState.EMITTED_END;
    }
  }


  private void fill() {
    long start = System.currentTimeMillis();
    Long offset;

    // Are there failed tuples? If so, fetch those first.
    offset = this.failedMsgRetryManager.nextFailedMessageToRetry();
    final boolean processingNewTuples = offset == null;
    if (processingNewTuples) {
      offset = emittedToOffset;
    }

    ByteBufferMessageSet msgs = null;
    try {
      msgs = KafkaUtils.fetchMessages(spoutConfig, consumer, partition, offset);
    } catch (TopicOffsetOutOfRangeException e) {
      offset = KafkaUtils.getOffset(consumer, partition.topic,
          partition.partition, kafka.api.OffsetRequest.EarliestTime());
      // fetch failed, so don't update the fetch metrics

      //fix bug [STORM-643] : remove outdated failed offsets
      if (!processingNewTuples) {
        // For the case of EarliestTime it would be better to discard
        // all the failed offsets, that are earlier than actual EarliestTime
        // offset, since they are anyway not there.
        // These calls to broker API will be then saved.
        Set<Long> omitted = this.failedMsgRetryManager.clearOffsetsBefore(offset);

        // Omitted messages have not been acked and may be lost
        if (null != omitted) {
          lostMessageCount.incrBy(omitted.size());
        }

        LOG.warn("Removing the failed offsets for {} that are out of range: {}",
            partition, omitted);
      }

      if (offset > emittedToOffset) {
        lostMessageCount.incrBy(offset - emittedToOffset);
        emittedToOffset = offset;
        LOG.warn("{} Using new offset: {}", partition, emittedToOffset);
      }

      return;
    }
    long millis = System.currentTimeMillis() - start;
    fetchAPILatencyMax.update(millis);
    fetchAPILatencyMean.update(millis);
    fetchAPICallCount.incr();
    if (msgs != null) {
      int numMessages = 0;

      for (MessageAndOffset msg : msgs) {
        final Long curOffset = msg.offset();
        if (curOffset < offset) {
          // Skip any old offsets.
          continue;
        }
        if (processingNewTuples || this.failedMsgRetryManager.shouldReEmitMsg(curOffset)) {
          numMessages += 1;
          if (!pending.containsKey(curOffset)) {
            pending.put(curOffset, System.currentTimeMillis());
          }
          waitingToEmit.add(msg);
          emittedToOffset = Math.max(msg.nextOffset(), emittedToOffset);
          if (failedMsgRetryManager.shouldReEmitMsg(curOffset)) {
            this.failedMsgRetryManager.retryStarted(curOffset);
          }
        }
      }
      fetchAPIMessageCount.incrBy(numMessages);
    }
  }

  public void ack(Long offset) {
    if (!pending.isEmpty() && pending.firstKey() < offset - spoutConfig.maxOffsetBehind) {
      // Too many things pending!
      pending.headMap(offset - spoutConfig.maxOffsetBehind).clear();
    }
    pending.remove(offset);
    this.failedMsgRetryManager.acked(offset);
    numberAcked++;
  }

  public void fail(Long offset) {
    if (offset < emittedToOffset - spoutConfig.maxOffsetBehind) {
      LOG.info(
          "Skipping failed tuple at offset={}"
              + " because it's more than maxOffsetBehind={}"
              + " behind emittedToOffset={} for {}",
          offset,
          spoutConfig.maxOffsetBehind,
          emittedToOffset,
          partition
      );
    } else {
      LOG.debug("Failing at offset={} with pending.size()={} and emittedToOffset={} for {}",
          offset, pending.size(), emittedToOffset, partition);
      numberFailed++;
      if (numberAcked == 0 && numberFailed > spoutConfig.maxOffsetBehind) {
        throw new RuntimeException("Too many tuple failures");
      }

      // Offset may not be considered for retry by failedMsgRetryManager
      if (this.failedMsgRetryManager.retryFurther(offset)) {
        this.failedMsgRetryManager.failed(offset);
      } else {
        // state for the offset should be cleaned up
        LOG.warn("Will not retry failed kafka offset {} further", offset);
        messageIneligibleForRetryCount.incr();
        pending.remove(offset);
        this.failedMsgRetryManager.acked(offset);
      }
    }
  }

  public void commit() {
    long lastCompletedOffset = lastCompletedOffset();
    if (committedTo != lastCompletedOffset) {
      LOG.debug("Writing last completed offset ({}) to ZK for {} for topology: {}",
          lastCompletedOffset, partition, topologyInstanceId);
      Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
          .put("topology", ImmutableMap.of("id", topologyInstanceId,
              "name", stormConf.get(Config.TOPOLOGY_NAME)))
          .put("offset", lastCompletedOffset)
          .put("partition", partition.partition)
          .put("broker", ImmutableMap.of("host", partition.host.host,
              "port", partition.host.port))
          .put("topic", partition.topic).build();
      state.writeJSON(committedPath(), data);

      committedTo = lastCompletedOffset;
      LOG.debug("Wrote last completed offset ({}) to ZK for {} for topology: {}",
          lastCompletedOffset, partition, topologyInstanceId);
    } else {
      LOG.debug("No new offset for {} for topology: {}", partition, topologyInstanceId);
    }
  }

  private String committedPath() {
    return spoutConfig.zkRoot + "/" + spoutConfig.id + "/" + partition.getId();
  }

  public long lastCompletedOffset() {
    if (pending.isEmpty()) {
      return emittedToOffset;
    } else {
      return pending.firstKey();
    }
  }

  public OffsetData getOffsetData() {
    return new OffsetData(emittedToOffset, lastCompletedOffset());
  }

  public Partition getPartition() {
    return partition;
  }

  public void close() {
    commit();
    connections.unregister(partition.host, partition.topic, partition.partition);
  }

  static class KafkaMessageId implements Serializable {
    private static final long serialVersionUID = 4962830658778031020L;
    public Partition partition;
    public long offset;

    KafkaMessageId(Partition partition, long offset) {
      this.partition = partition;
      this.offset = offset;
    }
  }

  public static class OffsetData {
    public long latestEmittedOffset;
    public long latestCompletedOffset;

    public OffsetData(long latestEmittedOffset, long latestCompletedOffset) {
      this.latestEmittedOffset = latestEmittedOffset;
      this.latestCompletedOffset = latestCompletedOffset;
    }
  }
}
