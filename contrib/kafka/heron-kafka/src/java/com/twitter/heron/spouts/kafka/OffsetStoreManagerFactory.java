package com.twitter.heron.spouts.kafka;

import com.twitter.heron.spouts.kafka.common.IOffsetStoreManager;

public class OffsetStoreManagerFactory {

    public final static String KAFKA_STORE = "kafka";

    private SpoutConfig spoutConfig;

    public OffsetStoreManagerFactory(SpoutConfig spoutConfig) {
        this.spoutConfig = spoutConfig;
    }

    public IOffsetStoreManager get() {
        if (spoutConfig.offsetStore != null && !spoutConfig.offsetStore.isEmpty()) {
            if (spoutConfig.offsetStore.equalsIgnoreCase(KAFKA_STORE)) {
                return new KafkaOffsetStoreManager(spoutConfig);
            } else {
                throw new RuntimeException("Unknown offset store specified: " + spoutConfig.offsetStore);
            }
        }

        throw new RuntimeException("Offset store should be specified");
    }
}
