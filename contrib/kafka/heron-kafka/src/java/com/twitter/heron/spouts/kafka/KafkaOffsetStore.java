package com.twitter.heron.spouts.kafka;

import com.google.common.collect.Maps;
import com.twitter.heron.spouts.kafka.common.GlobalPartitionId;
import com.twitter.heron.storage.MetadataStore;
import com.twitter.heron.storage.StoreSerializer;
import kafka.cluster.BrokerEndPoint;
import kafka.common.*;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaOffsetStore extends MetadataStore {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetStore.class);

    private SpoutConfig _spoutConfig;
    private GlobalPartitionId _id;

    private String _consumerGroupId;
    private String _consumerClientId;
    private int _stateOpTimeout;
    private int _stateOpMaxRetry;

    private int _correlationId = 0;
    private BlockingChannel _offsetManager;

    public KafkaOffsetStore(SpoutConfig spoutConfig, GlobalPartitionId id) {
        this._spoutConfig = spoutConfig;
        this._id = id;
        this._consumerClientId = _spoutConfig.clientId;
        this._stateOpTimeout = _spoutConfig.stateOpTimeout;
        this._stateOpMaxRetry = _spoutConfig.stateOpMaxRetry;
    }

    @Override
    public boolean initialize(String paramUId, String paramTopologyName, String paramComponentId,
                              StoreSerializer paramSerializer) {
        boolean res = super.initialize(paramUId, paramTopologyName, paramComponentId, paramSerializer);
        _consumerGroupId = keyPrefix;
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
        if (_offsetManager != null) {
            _offsetManager.disconnect();
            _offsetManager = null;
        }
    }

    // as there is a manager per topic per partition and the stateUpdateIntervalMs should not be too small
    // feels ok to  place a sync here
    private synchronized BlockingChannel locateOffsetManager() {
        if (_offsetManager == null) {
            BlockingChannel channel = new BlockingChannel(_id.host, _id.port,
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    1000000 // read timeout in millis
);
            channel.connect();

            GroupCoordinatorResponse groupCoordinatorResponse = null;
            long backoffMillis = 3000L;
            int maxRetry = 3;
            int retryCount = 0;
            // this usually only happens when the internal offsets topic does not exist before and we need to wait until
            // the topic is automatically created and the meta data are populated across cluster. So we hard-code the retry here.

            // one scenario when this could happen is during unit test.
            while (retryCount < maxRetry) {
                channel.send(new kafka.api.GroupCoordinatorRequest(_consumerGroupId, kafka.api.GroupCoordinatorRequest.CurrentVersion(),
                        _correlationId++, _consumerClientId));
                groupCoordinatorResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload());
                if (groupCoordinatorResponse.errorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                    LOG.warn("Failed to get coordinator: " + groupCoordinatorResponse.errorCode());
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

            if (groupCoordinatorResponse.errorCode() == ErrorMapping.NoError()) {
                BrokerEndPoint offsetManager = groupCoordinatorResponse.coordinator();
                if (!offsetManager.host().equals(_id.host)
                        || !(offsetManager.port() == _id.port)) {
                    // if the coordinator is different, from the above channel's host then reconnect
                    channel.disconnect();
                    channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                            BlockingChannel.UseDefaultBufferSize(),
                            BlockingChannel.UseDefaultBufferSize(),
                            _stateOpTimeout // read timeout in millis
);
                    channel.connect();
                }
            } else {
                throw new RuntimeException("Kafka metadata fetch error: " + groupCoordinatorResponse.errorCode());
            }
            _offsetManager = channel;
        }
        return _offsetManager;
    }

    private String attemptToRead() {
        List<TopicAndPartition> partitions = new ArrayList<>();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, _id.partition);
        partitions.add(thisTopicPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                _consumerGroupId,
                partitions,
                (short) 1, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                _correlationId++,
                _consumerClientId);

        BlockingChannel offsetManager = locateOffsetManager();
        offsetManager.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(offsetManager.receive().payload());
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
            _offsetManager = null;
            throw new RuntimeException("Kafka offset fetch error: " + result.error());
        }
    }

    private String read() {
        int attemptCount = 0;
        while (true) {
            try {
                return attemptToRead();

            } catch (RuntimeException re) {
                if (++attemptCount > _stateOpMaxRetry) {
                    _offsetManager = null;
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _stateOpMaxRetry
                            + ". Failed to fetch state for partition " + _id.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset fetch error: " + re.getMessage());
                }
            }
        }
    }

    private void attemptToWrite(long offsetOfPartition, String data) {
        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = Maps.newLinkedHashMap();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, _id.partition);
        offsets.put(thisTopicPartition, new OffsetAndMetadata(
                new OffsetMetadata(offsetOfPartition, data),
                now,
                Long.MAX_VALUE));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                _consumerGroupId,
                offsets,
                _correlationId,
                _consumerClientId,
                (short) 1); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

        BlockingChannel offsetManager = locateOffsetManager();
        offsetManager.send(commitRequest.underlying());
        OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(offsetManager.receive().payload());
        if (commitResponse.hasError()) {
            // note: here we should have only 1 error for the partition in request
            for (Object partitionErrorCode : commitResponse.errors().values()) {
                if (partitionErrorCode.equals(ErrorMapping.OffsetMetadataTooLargeCode())) {
                    throw new RuntimeException("Data is too big. The data object is " + data);
                } else {
                    _offsetManager = null;
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

            } catch(RuntimeException re) {
                if (++attemptCount > _stateOpMaxRetry) {
                    _offsetManager = null;
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _stateOpMaxRetry
                            + ". Failed to save state for partition " + _id.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset commit error: " + re.getMessage());
                }
            }
        }
    }
}
