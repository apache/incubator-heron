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

import com.amazonaws.services.kinesis.stormspout.exceptions.InvalidSeekPositionException;

/**
 * Fetches data from a shard.
 */
public interface IShardGetter {
    /**
     * Get at most maxNumberOfRecords.
     *
     * @param maxNumberOfRecords Max number of records to get.
     * @return data records
     */
    Records getNext(int maxNumberOfRecords);

    /**
     * Seek to a position in the shard. All subsequent calls to getNext will read starting
     * from the specified position.
     * Note: This is expected to be invoked infrequently (e.g. some implementations may not support a high call rate).
     *
     * @param position  Position to seek to.
     * @throws InvalidSeekPositionException if the position is invalid (e.g. sequence number not valid for the shard).
     */
    void seek(ShardPosition position) throws InvalidSeekPositionException;

    /**
     * @return shardId of the shard this getter reads from.
     */
    String getAssociatedShard();
}
