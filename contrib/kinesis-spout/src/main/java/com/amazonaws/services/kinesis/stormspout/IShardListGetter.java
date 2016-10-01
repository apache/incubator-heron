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

import java.io.Serializable;

import com.google.common.collect.ImmutableSortedMap;

/**
 * Used to list the shards in the stream.
 */
public interface IShardListGetter extends Serializable {

    /**
     * Retrieve the list of shards for the stream.
     * 
     * @return a map of shardId to ShardInfo.
     */
    ImmutableSortedMap<String, ShardInfo> getShardList();
}
