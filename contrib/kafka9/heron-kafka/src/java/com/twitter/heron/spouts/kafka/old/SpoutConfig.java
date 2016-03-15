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

package com.twitter.heron.spouts.kafka.old;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.twitter.heron.api.spout.MultiScheme;
import com.twitter.heron.api.spout.RawMultiScheme;
import com.twitter.heron.spouts.kafka.common.KafkaConfig;
import com.twitter.heron.spouts.kafka.common.TimestampExtractor;

public class SpoutConfig implements Serializable {
  private static final long DEFAULT_TUPLE_IN_THE_PAST_THRESHOLD = 30000; // 30s

  public static final String TOPOLOGY_STORE_UPDATE_MSEC = "topology.store.update.msec";

  public static final String START_OFFSET_DELTA = "spout.offset.delta";
  /**
   * Array of filter operator's config like classname e.g
   * com.twitter.heron.spouts.kafka.LagAvoidanceFiler.
   * Name of spout, e.g spout0 Parameter e.g {}
   */
  public static final String TOPOLOGY_FILTER_CONFIG = "topology.filter.config";

  public static final String SPOUT_SHOULD_REPLAY_FAILED_OFFSETS =
      "spout.should.replay.failed.offsets";

  public static final String SPOUT_MAX_FAILED_OFFSETS = "spout.max.failed.offsets";

  public String topic;  // Required

  public KafkaConfig.BrokerHosts hosts;  // Required.

  public String clientId = "heron"; //OffsetRequest.DefaultClientId();

  public int fetchMaxWait = 10000;

  public List<String> zkServers = new ArrayList<String>();

  public int zkPort = 2181;

  public String zkRoot = "/kafkaconsumer";

  public String id = null;

  public int storeUpdateMsec = 2 * 1000;

  public int zookeeperStoreUpdateMsec = -1; // We do not write offsets to zk anymore

  public int zookeeperStoreSessionTimeout = 40000;

  public int zookeeperConnectionTimeout  = 15000;

  public com.twitter.heron.spouts.kafka.common.FilterOperator filterOperator;

  public boolean shouldAnchorEmits = true;

  public boolean shouldReplay = true;

  public int zookeeperRetryCount = 5;

  public int zookeeperRetryInterval = 60 * 1000;

  public TimestampExtractor timestampExtractor = null;

  public int emitQueueMaxSize = 1000;

  // If a timestamp extractor is specified, we extract the timestamps associated with all tuples we
  // read from Kafka partitions, and we keep track of the latest timestamp. If the timestamp
  // associated with a tuple is too far in the past compared to the latest timestamp, we increment
  // the tuplesInThePast counter. This constant determines how far in the past the tuple's timestamp
  // should be in order for the counter to be incremented. The default is 30 seconds.
  public long tupleInThePastThreshold = DEFAULT_TUPLE_IN_THE_PAST_THRESHOLD;

  public int fetchSizeBytes = 10 * 1024 * 1024;  // 10MB

  public MultiScheme scheme = new RawMultiScheme();

  public long startOffsetTime = -1;  // Default set to start from latest.

  public boolean forceFromStart = false;

  public int socketTimeoutMs = 1 * 60 * 1000;  // 1 min

  public int bufferSizeBytes = 10 * 1024 * 1024;  // 10 MB buffer

  public long maxFailedTupleCount = 10;  // Max failed tuple at a time. Anymore will be dropped.
  /*
   * Read the kafka queue with a fixed lag (in bytes).
   *
   * For example, if it is set to be 1048576 (1M). The kafka reader will
   * always keep a fixed lag 1M with the head of that kafka partition. If the queue
   * has 100 partitions, the storm job will keep a 100M lag with the head.
   *
   * Note that this job would slow down the storm topology a little bit. Please add some
   * workers if you would like to use this feature.
   *
   * By default, it is set to be -1. So this feature is disabled.
   */
  public long fixedLagBytes = -1;

  // offset state information storage. validate options are storm and kafka
  public String offsetStore = OffsetStoreManagerFactory.KAFKA_STORE;
  // timeout in millis for state read/write operations
  public int stateOpTimeout = 5000;
  // max retries allowed for state read/write operations
  public int stateOpMaxRetry = 3;

  /**
   * Ctor. Create a config to be passed to kafka spout containing broker information and id.
   */
  public SpoutConfig(KafkaConfig.BrokerHosts hosts, String topic, String zkRoot, String id) {
    this.hosts = hosts;
    this.topic = topic;
    if (zkRoot != null) {
      this.zkRoot = zkRoot;
    }
    this.id = id;
  }

  /**
   * Configures the spout to start reading Kafka partitions at the offset that stores the events
   * added at time {@code earliestEventTimestamp}.
   *
   * There are two special value for the {@code earliestEventTimestamp}:
   *   -1: will force the spout to start reading from the latest offset.
   *   -2: will force the spout to start reading from the earliest offset.
   *
   * @param earliestEventTimestamp The timestamp (in milliseconds) when the earliest event was added
   *                               to the Kafka topic.
   */
  public void forceStartOffsetTime(long earliestEventTimestamp) {
    startOffsetTime = earliestEventTimestamp;
    forceFromStart = true;
  }

  /** Create kafka cnxns using zookeeper for service discovery. */
  public static class ZkHosts implements KafkaConfig.BrokerHosts {
    public String brokerZkStr = null;
    public String brokerZkPath = null; // e.g., /kafka/brokers
    public int refreshFreqMSecs = 120 * 1000;

    public ZkHosts(String brokerZkStr, String brokerZkPath) {
      this.brokerZkStr = brokerZkStr;
      this.brokerZkPath = brokerZkPath;
    }
  }
}
