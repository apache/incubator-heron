package com.twitter.heron.spouts.kafka;

import com.twitter.heron.spouts.kafka.common.GlobalPartitionId;
import com.twitter.heron.spouts.kafka.common.IOffsetStoreManager;

public class KafkaOffsetStoreManager implements IOffsetStoreManager<KafkaOffsetStore> {

    private SpoutConfig spoutConfig;

    public KafkaOffsetStoreManager(SpoutConfig spoutConfig) {
        this.spoutConfig = spoutConfig;
    }

    @Override
    public KafkaOffsetStore getStore(GlobalPartitionId id) {
        return new KafkaOffsetStore(spoutConfig, id);
    }

    @Override
    public void close(KafkaOffsetStore store) {
        store.close();
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
