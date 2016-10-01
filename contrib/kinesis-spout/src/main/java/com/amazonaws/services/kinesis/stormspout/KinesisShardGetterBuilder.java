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

import com.google.common.collect.ImmutableList;

/**
 * Builds KinesisShardGetters.
 */
class KinesisShardGetterBuilder implements IShardGetterBuilder {
    private static final long serialVersionUID = 6038308016758172991L;

    private final int maxRecordsPerCall;
    private final long emptyRecordListBackoffMillis;

    private final String streamName;
    private final KinesisHelper helper;

    /**
     * Constructor.
     * 
     * @param streamName Kinesis stream to create the getters in.
     * @param helper Used to get the AmazonKinesisClient object (used by the getters).
     */
    public KinesisShardGetterBuilder(final String streamName,
            final KinesisHelper helper,
            final int maxRecordsPerCall,
            final long emptyRecordListBackoffMillis) {
        this.streamName = streamName;
        this.helper = helper;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.emptyRecordListBackoffMillis = emptyRecordListBackoffMillis;
    }

    @Override
    public ImmutableList<IShardGetter> buildGetters(ImmutableList<String> shardAssignment) {
        ImmutableList.Builder<IShardGetter> builder = new ImmutableList.Builder<>();

        for (String shard : shardAssignment) {
            builder.add(new BufferedGetter(new KinesisShardGetter(streamName, shard, helper.getSharedkinesisClient()),
                    maxRecordsPerCall,
                    emptyRecordListBackoffMillis));
        }

        return builder.build();
    }
}
