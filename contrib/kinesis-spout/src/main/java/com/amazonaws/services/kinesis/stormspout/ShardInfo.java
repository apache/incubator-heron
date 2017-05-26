/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * In this class we track the children for a shard.
 */
class ShardInfo {
    private final String shardId;
    private String mergesInto;
    private List<String> splitsInto;

    /**
     * Creates a new ShardInfo representing a shard that does not split or merge.
     * 
     * @param shardId the Kinesis shard ID.
     */
    ShardInfo(String shardId) {
        this.shardId = shardId;
        this.mergesInto = "";
        this.splitsInto = new ArrayList<>(2);
    }

    /**
     * @return the shard ID.
     */
    String getShardId() {
        return shardId;
    }

    /**
     * Define what the shard merges into. This is meant to be called at most once (not idempotent).
     * Cannot be called if addSplitsInto has been called.
     * 
     * @param mergeShardId set the shard ID that getShardId() merges into.
     */
    void setMergesInto(String mergeShardId) {
        assert this.splitsInto.isEmpty() : "A shard cannot merge after a split.";
        assert this.mergesInto.isEmpty() : "A shard cannot merge twice.";
        this.mergesInto = mergeShardId;
    }

    /**
     * Define what the shard splits into. Can be called an arbitrary amount of times. Cannot be
     * called if setMergesInto has been called.
     * 
     * @param splitShardId add a shard ID to the list of shards getShardId() splits into.
     */
    void addSplitsInto(String splitShardId) {
        assert this.mergesInto.isEmpty() : "A shard cannot split after a merge.";
        this.splitsInto.add(splitShardId);
    }

    /**
     * @return the shard ID of the shard that getShardId() merges into. Empty string if the shard
     *         does not merge into another shard.
     */
    String getMergesInto() {
        return mergesInto;
    }

    /**
     * @return immutable view of the shards resulting from the split. Empty list if the shard does
     *         not split.
     */
    List<String> getSplitsInto() {
        return Collections.unmodifiableList(splitsInto);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
