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

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.ImmutableList;

/**
 * Used to hold a set of records and indicate if we reached the end of a shard.
 * Used in IShardGetter.getNext(n).
 */
class Records {
    private final ImmutableList<Record> records;
    private final boolean endOfShard;

    /**
     * Constructor.
     * 
     * @param records Kinesis records
     * @param endOfShard Did we reach the end of the shard?
     */
    Records(final ImmutableList<Record> records, final boolean endOfShard) {
        this.records = records;
        this.endOfShard = endOfShard;
    }

    /**
     * @return a new empty set of records for a shard.
     */
    static Records empty() {
        return empty(false);
    }

    /**
     * @param closed Is the shard closed?
     * @return a new empty set of records for an open or closed shard.
     */
    static Records empty(final boolean closed) {
        return new Records(ImmutableList.<Record> of(), closed);
    }

    /**
     * @return the immutable list of records.
     */
    ImmutableList<Record> getRecords() {
        return records;
    }

    /**
     * @return true if we reached the end of a shard.
     */
    boolean isEndOfShard() {
        return endOfShard;
    }

    /**
     * Does the Records instance contain records?
     * 
     * @return true if getRecords() has records.
     */
    boolean isEmpty() {
        return records.isEmpty();
    }
}
