// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spouts.kafka.old;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.spouts.kafka.common.GlobalPartitionId;
import com.twitter.heron.storage.MetadataStore;
import com.twitter.heron.storage.StoreSerializer;

// CHECKSTYLE:OFF AvoidStarImport
import kafka.api.ConsumerMetadataRequest;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;

@SuppressWarnings({"unchecked", "rawtypes"})
// CHECKSTYLE:OFF IllegalCatch
public class KafkaOffsetStore extends MetadataStore {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetStore.class);

  private SpoutConfig storeSpoutConfig;
  private GlobalPartitionId globPartitionId;

  private String storeConsumerGroupId;
  private String storeConsumerClientId;
  private int storeStateOpTimeout;
  private int storeStateOpMaxRetry;

  private int storeCorrelationId = 0;
  private BlockingChannel storeOffsetManager;

  public KafkaOffsetStore(SpoutConfig spoutConfig, GlobalPartitionId id) {
    this.storeSpoutConfig = spoutConfig;
    this.globPartitionId = id;
    this.storeConsumerClientId = storeSpoutConfig.clientId;
    this.storeStateOpTimeout = storeSpoutConfig.stateOpTimeout;
    this.storeStateOpMaxRetry = storeSpoutConfig.stateOpMaxRetry;
  }

  @Override
  public boolean initialize(String paramUId, String paramTopologyName, String paramComponentId,
                            StoreSerializer paramSerializer) {
    boolean res = super.initialize(paramUId, paramTopologyName, paramComponentId, paramSerializer);
    storeConsumerGroupId = keyPrefix;
    return res;
  }

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public <T> boolean setPKey(String keySuffix, T value) {
    Map<Object, Object> state = (Map<Object, Object>) value;

    assert state.containsKey("offset");

    Long offsetOfPartition = (Long) state.get("offset");
    String stateData = JSONValue.toJSONString(state);
    write(offsetOfPartition, stateData);
    return true;
  }

  @Override
  public <T> boolean setLKey(String pKey, String lKey, T value) {
    return false;
  }

  @Override
  public byte[] readPKey(String keySuffix) {
    try {
      return read().getBytes("UTF-8");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }

  @Override
  public byte[] readLKey(String pKey, String lKey) {
    return new byte[0];
  }

  @Override
  public Map<String, byte[]> readAllLKey(String pKey) {
    return null;
  }

  @Override
  public void close() {
    if (storeOffsetManager != null) {
      storeOffsetManager.disconnect();
      storeOffsetManager = null;
    }
  }

  // as there is a manager per topic per partition and the stateUpdateIntervalMs should not be too
  // small
  // feels ok to  place a sync here
  private synchronized BlockingChannel locateOffsetManager() {
    if (storeOffsetManager == null) {
      BlockingChannel channel = new BlockingChannel(globPartitionId.host, globPartitionId.port,
          BlockingChannel.UseDefaultBufferSize(),
          BlockingChannel.UseDefaultBufferSize(),
          1000000 /* read timeout in millis */
      );
      channel.connect();

      ConsumerMetadataResponse metadataResponse = null;
      long backoffMillis = 3000L;
      int maxRetry = 3;
      int retryCount = 0;
      // this usually only happens when the internal offsets topic does not exist before and we need
      // to wait until
      // the topic is automatically created and the meta data are populated across cluster. So we
      // hard-code the retry here.

      // one scenario when this could happen is during unit test.
      while (retryCount < maxRetry) {
        channel.send(new ConsumerMetadataRequest(storeConsumerGroupId, ConsumerMetadataRequest
            .CurrentVersion(),
            storeCorrelationId++, storeConsumerClientId));
        metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
        if (metadataResponse.errorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
          LOG.warn("Failed to get coordinator: " + metadataResponse.errorCode());
          retryCount++;
          try {
            Thread.sleep(backoffMillis);
          } catch (InterruptedException e) {
            // eat the exception
          }
        } else {
          break;
        }
      }

      if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
        kafka.cluster.Broker offsetManager = metadataResponse.coordinator();
        if (!offsetManager.host().equals(globPartitionId.host)
            || !(offsetManager.port() == globPartitionId.port)) {
          // if the coordinator is different, from the above channel's host then reconnect
          channel.disconnect();
          channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
              BlockingChannel.UseDefaultBufferSize(),
              BlockingChannel.UseDefaultBufferSize(),
              storeStateOpTimeout /* read timeout in millis */
          );
          channel.connect();
        }
      } else {
        throw new RuntimeException("Kafka metadata fetch error: " + metadataResponse.errorCode());
      }
      storeOffsetManager = channel;
    }
    return storeOffsetManager;
  }

  private String attemptToRead() {
    List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
    TopicAndPartition thisTopicPartition = new TopicAndPartition(storeSpoutConfig.topic,
        globPartitionId.partition);
    partitions.add(thisTopicPartition);
    OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
        storeConsumerGroupId,
        partitions,
        (short) 1, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        storeCorrelationId++,
        storeConsumerClientId);

    BlockingChannel offsetManager = locateOffsetManager();
    offsetManager.send(fetchRequest.underlying());
    OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(offsetManager.receive()
        .buffer());
    OffsetMetadataAndError result = fetchResponse.offsets().get(thisTopicPartition);
    if (result.error() == ErrorMapping.NoError()) {
      String retrievedMetadata = result.metadata();
      if (retrievedMetadata != null) {
        return retrievedMetadata;
      } else {
        // let it return null, this maybe the first time it is called before the state is persisted
        return null;
      }

    } else {
      storeOffsetManager = null;
      throw new RuntimeException("Kafka offset fetch error: " + result.error());
    }
  }

  private String read() {
    int attemptCount = 0;
    while (true) {
      try {
        return attemptToRead();
      } catch (RuntimeException re) {
        if (++attemptCount > storeStateOpMaxRetry) {
          storeOffsetManager = null;
          throw re;
        } else {
          LOG.warn("Attempt " + attemptCount + " out of " + storeStateOpMaxRetry
              + ". Failed to fetch state for partition " + globPartitionId.partition
              + " of topic " + storeSpoutConfig.topic + " due to Kafka offset fetch error: " + re
              .getMessage());
        }
      }
    }
  }

  private void attemptToWrite(long offsetOfPartition, String data) {
    long now = System.currentTimeMillis();
    Map<TopicAndPartition, OffsetAndMetadata> offsets = Maps.newLinkedHashMap();
    TopicAndPartition thisTopicPartition = new TopicAndPartition(storeSpoutConfig.topic,
        globPartitionId.partition);
    offsets.put(thisTopicPartition, new OffsetAndMetadata(
        offsetOfPartition,
        data,
        now));
    OffsetCommitRequest commitRequest = new OffsetCommitRequest(
        storeConsumerGroupId,
        offsets,
        storeCorrelationId,
        storeConsumerClientId,
        (short) 1); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

    BlockingChannel offsetManager = locateOffsetManager();
    offsetManager.send(commitRequest.underlying());
    OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(offsetManager.receive()
        .buffer());
    if (commitResponse.hasError()) {
      // note: here we should have only 1 error for the partition in request
      for (Object partitionErrorCode : commitResponse.errors().values()) {
        if (partitionErrorCode.equals(ErrorMapping.OffsetMetadataTooLargeCode())) {
          throw new RuntimeException("Data is too big. The data object is " + data);
        } else {
          storeOffsetManager = null;
          throw new RuntimeException("Kafka offset commit error: " + partitionErrorCode);
        }
      }
    }
  }

  private void write(Long offsetOfPartition, String data) {
    int attemptCount = 0;
    while (true) {
      try {
        attemptToWrite(offsetOfPartition, data);
        return;
      } catch (RuntimeException re) {
        if (++attemptCount > storeStateOpMaxRetry) {
          storeOffsetManager = null;
          throw re;
        } else {
          LOG.warn("Attempt " + attemptCount + " out of " + storeStateOpMaxRetry
              + ". Failed to save state for partition " + globPartitionId.partition
              + " of topic " + storeSpoutConfig.topic + " due to Kafka offset commit error: "
              + re.getMessage());
        }
      }
    }
  }
}
