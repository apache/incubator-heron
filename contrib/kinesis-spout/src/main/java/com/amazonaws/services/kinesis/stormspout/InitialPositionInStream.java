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

/**
 * Used to specify the position in the stream where a new topology should start from.
 * This is used during bootstrap (if a checkpoint doesn't exist for a shard).
 */
public enum InitialPositionInStream {

    /**
     * Start after the most recent data record (fetch new data).
     */
    LATEST,

    /**
     * Start from the oldest available data record.
     */
    TRIM_HORIZON;
}
