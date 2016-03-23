package com.twitter.heron.spouts.kafka.old;


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
