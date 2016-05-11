package com.twitter.heron.spouts.kafka;

import com.twitter.heron.api.spout.MultiScheme;
import com.twitter.heron.api.spout.RawMultiScheme;
import com.twitter.heron.spouts.kafka.common.FilterOperator;
import com.twitter.heron.spouts.kafka.common.KafkaConfig;
import com.twitter.heron.spouts.kafka.common.TimestampExtractor;

import java.io.Serializable;

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

    public String bootstrapBrokers;  // Required

    public KafkaConfig.BrokerHosts hosts;

    public String topic;  // Required

    public String id; // Required

    public String clientId = "heron";

    // offset state information storage. validate options are storm and kafka
    public String offsetStore = OffsetStoreManagerFactory.KAFKA_STORE;
    // timeout in millis for state read/write operations
    public int stateOpTimeout = 5000;
    // max retries allowed for state read/write operations
    public int stateOpMaxRetry = 3;

    public int storeUpdateMsec = 10 * 1000;

    public int zookeeperStoreUpdateMsec = -1; // We do not write offsets to zk anymore

    public int zookeeperStoreSessionTimeout = 40000;

    public FilterOperator filterOperator;

    public boolean shouldAnchorEmits = true;

    public int refreshFreqMSecs = 120 * 1000;

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

    /**
     * Indicates whether to start from earliest or latest offset in case if there was no previous run and offset couldn't be restored.
     */
    public boolean startFromLatestOffset = true;

    public int socketTimeoutMs = 1 * 60 * 1000;  // 1 min

    public int bufferSizeBytes = 10 * 1024 * 1024;  // 10 MB buffer

    public long maxFailedTupleCount = 10;  // Max failed tuple at a time. Anymore will be dropped

    public SpoutConfig(String topic, String bootstrapBrokers, String id) {
        this.topic = topic;
        this.bootstrapBrokers = bootstrapBrokers;
        this.id = id;
    }

    /**
     * Configures the spout to start reading Kafka partitions at the offset that stores the events
     * added at time {@code earliestEventTimestamp}.
     * <p>
     * There are two special value for the {@code earliestEventTimestamp}:
     * -1: will force the spout to start reading from the latest offset.
     * -2: will force the spout to start reading from the earliest offset.
     * <p>
     * NOTE: this will not actually set to start from the specified timestamp, because new consumer doesn't have an
     * exposed API, although if special value (-1,-2) is specified it will set corresponding parameter so that spout
     * starts consuming from the earliest or the latest offset
     *
     * @param earliestEventTimestamp The timestamp (in milliseconds) when the earliest event was added
     *                               to the Kafka topic.
     */
    public void forceStartOffsetTime(long earliestEventTimestamp) {
        if (earliestEventTimestamp == -1) {
            startFromLatestOffset = true;
        } else if (earliestEventTimestamp == -2) {
            startFromLatestOffset = false;
        }

        startOffsetTime = earliestEventTimestamp;
        forceFromStart = true;
    }

    public static class ZkHosts implements KafkaConfig.BrokerHosts {
        private static final String DEFAULT_ZK_PATH = "/brokers";

        public String brokerZkStr = null;
        public String brokerZkPath = null; // e.g., /kafka/brokers
        public int refreshFreqMSecs = 120 * 1000;

        public ZkHosts(String brokerZkStr, String brokerZkPath) {
            this.brokerZkStr = brokerZkStr;
            this.brokerZkPath = brokerZkPath;
        }

        public ZkHosts(String brokerZkStr) {
            this(brokerZkStr, DEFAULT_ZK_PATH);
        }
    }
}
