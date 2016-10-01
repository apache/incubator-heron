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

package com.amazonaws.services.kinesis.stormspout.state.zookeeper;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Used for JSON serialization/deserialization of shard list state.
 */
class ShardListV0 {

    private ImmutableList<String> shards = new ImmutableList.Builder<String>().build(); 
    
    public ShardListV0(List<String> shards) {
        this.shards = ImmutableList.copyOf(shards);
    }
    
    public ShardListV0() {        
    }
    
    public List<String> getShardList() {
        return shards;
    }
    
    public void setShardList(List<String> shards) {
        this.shards = ImmutableList.copyOf(shards);
    }

}
