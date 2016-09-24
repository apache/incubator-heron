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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import org.apache.kafka.common.utils.Utils;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.kafka.trident.IBrokerReader;
import org.apache.storm.kafka.trident.StaticBrokerReader;
import org.apache.storm.kafka.trident.ZkBrokerReader;
import org.apache.storm.metric.api.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

public final class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
  private static final int NO_OFFSET = -5;

  //suppress default constructor for noninstantiablility
  private KafkaUtils() {
    throw new AssertionError();
  }

  public static IBrokerReader makeBrokerReader(Map<String, Object> stormConf, KafkaConfig conf) {
    if (conf.hosts instanceof StaticHosts) {
      return new StaticBrokerReader(conf.topic,
          ((StaticHosts) conf.hosts).getPartitionInformation());
    } else {
      return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
    }
  }


  public static long getOffset(SimpleConsumer consumer,
                               String topic,
                               int partition,
                               KafkaConfig config) {
    long startOffsetTime = config.startOffsetTime;
    return getOffset(consumer, topic, partition, startOffsetTime);
  }

  public static long getOffset(SimpleConsumer consumer,
                               String topic,
                               int partition,
                               long startOffsetTime) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
    if (offsets.length > 0) {
      return offsets[0];
    } else {
      return NO_OFFSET;
    }
  }

  public static class KafkaOffsetMetric implements IMetric {
    private Map<Partition, PartitionManager.OffsetData> partitionToOffset =
        new HashMap<Partition, PartitionManager.OffsetData>();
    private Set<Partition> partitions;
    private DynamicPartitionConnections connections;

    public KafkaOffsetMetric(DynamicPartitionConnections connections) {
      this.connections = connections;
    }

    public void setOffsetData(Partition partition, PartitionManager.OffsetData offsetData) {
      partitionToOffset.put(partition, offsetData);
    }

    private class TopicMetrics {
      private long totalSpoutLag = 0;
      private long totalEarliestTimeOffset = 0;
      private long totalLatestTimeOffset = 0;
      private long totalLatestEmittedOffset = 0;
      private long totalLatestCompletedOffset = 0;
    }

    @Override
    public Object getValueAndReset() {
      try {
        HashMap<String, Long> ret = new HashMap<>();
        if (partitions != null && partitions.size() == partitionToOffset.size()) {
          Map<String, TopicMetrics> topicMetricsMap = new TreeMap<String, TopicMetrics>();
          for (Map.Entry<Partition, PartitionManager.OffsetData> e
              : partitionToOffset.entrySet()) {
            Partition partition = e.getKey();
            SimpleConsumer consumer = connections.getConnection(partition);
            if (consumer == null) {
              LOG.warn("partitionToOffset contains partition not found in connections. "
                  + "Stale partition data?");
              return null;
            }
            long latestTimeOffset = getOffset(consumer, partition.topic, partition.partition,
                kafka.api.OffsetRequest.LatestTime());
            long earliestTimeOffset = getOffset(consumer, partition.topic, partition.partition,
                kafka.api.OffsetRequest.EarliestTime());
            if (latestTimeOffset == KafkaUtils.NO_OFFSET) {
              LOG.warn("No data found in Kafka Partition " + partition.getId());
              return null;
            }
            long latestEmittedOffset = e.getValue().latestEmittedOffset;
            long latestCompletedOffset = e.getValue().latestCompletedOffset;
            long spoutLag = latestTimeOffset - latestCompletedOffset;
            String topic = partition.topic;
            String metricPath = partition.getId();
            //Handle the case where Partition Path Id does not contain topic name
            // Partition.getId() == "partition_" + partition
            if (!metricPath.startsWith(topic + "/")) {
              metricPath = topic + "/" + metricPath;
            }
            ret.put(metricPath + "/" + "spoutLag", spoutLag);
            ret.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffset);
            ret.put(metricPath + "/" + "latestTimeOffset", latestTimeOffset);
            ret.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffset);
            ret.put(metricPath + "/" + "latestCompletedOffset", latestCompletedOffset);

            if (!topicMetricsMap.containsKey(partition.topic)) {
              topicMetricsMap.put(partition.topic, new TopicMetrics());
            }

            TopicMetrics topicMetrics = topicMetricsMap.get(partition.topic);
            topicMetrics.totalSpoutLag += spoutLag;
            topicMetrics.totalEarliestTimeOffset += earliestTimeOffset;
            topicMetrics.totalLatestTimeOffset += latestTimeOffset;
            topicMetrics.totalLatestEmittedOffset += latestEmittedOffset;
            topicMetrics.totalLatestCompletedOffset += latestCompletedOffset;
          }

          for (Map.Entry<String, TopicMetrics> e : topicMetricsMap.entrySet()) {
            String topic = e.getKey();
            TopicMetrics topicMetrics = e.getValue();
            ret.put(topic + "/" + "totalSpoutLag", topicMetrics.totalSpoutLag);
            ret.put(topic + "/" + "totalEarliestTimeOffset", topicMetrics.totalEarliestTimeOffset);
            ret.put(topic + "/" + "totalLatestTimeOffset", topicMetrics.totalLatestTimeOffset);
            ret.put(topic + "/" + "totalLatestEmittedOffset",
                topicMetrics.totalLatestEmittedOffset);
            ret.put(topic + "/" + "totalLatestCompletedOffset",
                topicMetrics.totalLatestCompletedOffset);
          }

          return ret;
        } else {
          LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
        }
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Throwable t) {
        LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
      }
      return null;
    }

    public void refreshPartitions(Set<Partition> newPartitions) {
      partitions = newPartitions;
      Iterator<Partition> it = partitionToOffset.keySet().iterator();
      while (it.hasNext()) {
        if (!newPartitions.contains(it.next())) {
          it.remove();
        }
      }
    }
  }

  /***
   * Fetch messages from kafka
   * @param config
   * @param consumer
   * @param partition
   * @param offset
   * @return
   * @throws TopicOffsetOutOfRangeException
   * @throws FailedFetchException
   * @throws RuntimeException
   */
  public static ByteBufferMessageSet fetchMessages(KafkaConfig config,
                                                   SimpleConsumer consumer,
                                                   Partition partition,
                                                   long offset)
      throws TopicOffsetOutOfRangeException, FailedFetchException, RuntimeException {
    ByteBufferMessageSet msgs = null;
    String topic = partition.topic;
    int partitionId = partition.partition;
    FetchRequestBuilder builder = new FetchRequestBuilder();
    FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, config.fetchSizeBytes)
        .clientId(config.clientId)
        .maxWait(config.fetchMaxWait)
        .minBytes(config.minFetchByte)
        .build();
    FetchResponse fetchResponse;
    try {
      fetchResponse = consumer.fetch(fetchRequest);
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      if (e instanceof ConnectException
          || e instanceof SocketTimeoutException
          || e instanceof IOException
          || e instanceof UnresolvedAddressException
          ) {
        LOG.warn("Network error when fetching messages:", e);
        throw new FailedFetchException(e);
      } else {
        throw new RuntimeException(e);
      }
    }
    if (fetchResponse.hasError()) {
      KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
      if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)
          && config.useStartOffsetTimeIfOffsetOutOfRange) {
        String msg = partition + " Got fetch request with offset out of range: [" + offset + "]";
        LOG.warn(msg);
        throw new TopicOffsetOutOfRangeException(msg);
      } else {
        String message = "Error fetching data from [" + partition + "] for topic ["
            + topic + "]: [" + error + "]";
        LOG.error(message);
        throw new FailedFetchException(message);
      }
    } else {
      msgs = fetchResponse.messageSet(topic, partitionId);
    }
    return msgs;
  }


  public static Iterable<List<Object>> generateTuples(KafkaConfig kafkaConfig,
                                                      Message msg,
                                                      String topic) {
    Iterable<List<Object>> tups;
    ByteBuffer payload = msg.payload();
    if (payload == null) {
      return null;
    }

    byte[] payloadBytes = Utils.toArray(payload);
    ByteBuffer key = msg.key();
    if (key != null && kafkaConfig.scheme instanceof KeyValueSchemeAsMultiScheme) {
      byte[] keyBytes = Utils.toArray(key);
      tups = ((KeyValueSchemeAsMultiScheme) kafkaConfig.scheme)
          .deserializeKeyAndValue(keyBytes, payloadBytes);
    } else {
      if (kafkaConfig.scheme instanceof StringMultiSchemeWithTopic) {
        tups = ((StringMultiSchemeWithTopic) kafkaConfig.scheme)
            .deserializeWithTopic(topic, payloadBytes);
      } else {
        tups = kafkaConfig.scheme.deserialize(payloadBytes);
      }
    }
    return tups;
  }

  public static Iterable<List<Object>> generateTuples(MessageMetadataSchemeAsMultiScheme scheme,
                                                      Message msg,
                                                      Partition partition,
                                                      long offset) {
    ByteBuffer payload = msg.payload();
    if (payload == null) {
      return null;
    }
    return scheme.deserializeMessageWithMetadata(Utils.toArray(payload), partition, offset);
  }


  public static List<Partition> calculatePartitionsForTask(
      List<GlobalPartitionInformation> partitons,
      int totalTasks,
      int taskIndex) {
    Preconditions.checkArgument(taskIndex < totalTasks, "task index must be less that total tasks");
    List<Partition> taskPartitions = new ArrayList<Partition>();
    List<Partition> partitions = new ArrayList<Partition>();
    for (GlobalPartitionInformation partitionInformation : partitons) {
      partitions.addAll(partitionInformation.getOrderedPartitions());
    }
    int numPartitions = partitions.size();
    if (numPartitions < totalTasks) {
      LOG.warn("there are more tasks than partitions (tasks: " + totalTasks + "; partitions: "
          + numPartitions + "), some tasks will be idle");
    }
    for (int i = taskIndex; i < numPartitions; i += totalTasks) {
      Partition taskPartition = partitions.get(i);
      taskPartitions.add(taskPartition);
    }
    logPartitionMapping(totalTasks, taskIndex, taskPartitions);
    return taskPartitions;
  }

  private static void logPartitionMapping(int totalTasks,
                                          int taskIndex,
                                          List<Partition> taskPartitions) {
    String taskPrefix = taskId(taskIndex, totalTasks);
    if (taskPartitions.isEmpty()) {
      LOG.warn(taskPrefix + "no partitions assigned");
    } else {
      LOG.info(taskPrefix + "assigned " + taskPartitions);
    }
  }

  public static String taskId(int taskIndex, int totalTasks) {
    return "Task [" + (taskIndex + 1) + "/" + totalTasks + "] ";
  }
}
