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

package com.amazonaws.services.kinesis.stormspout.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares two shardIds based on their number after the '-'.
 */
public class ShardIdComparator implements Comparator<String>, Serializable {
    private static final long serialVersionUID = -5645750076315794599L;

    /** No-op constructor. */
    public ShardIdComparator() { }

    @Override
    public int compare(String shard1, String shard2) {
        Integer shard1Index = Integer.parseInt(shard1.split("-")[1]);
        Integer shard2Index = Integer.parseInt(shard2.split("-")[1]);
        return shard1Index.compareTo(shard2Index);
    }
}
