package com.twitter.heron.spouts.kafka.old;

import com.twitter.heron.storage.MetadataStore;

public interface IOffsetStoreManager<T extends MetadataStore> {

    T getStore(GlobalPartitionId id);

    void close(T store);

    void close();
}
