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

package com.amazonaws.services.kinesis.stormspout.exceptions;

import com.amazonaws.services.kinesis.stormspout.ShardPosition;

/**
 * Thrown when the specified seek position in the shard is invalid.
 * E.g. specified sequence number is invalid for this shard/stream.
 */
public class InvalidSeekPositionException extends Exception {
    private static final long serialVersionUID = 6965780387493774707L;

    private final ShardPosition position;

    /**
     * @param position Shard position which we could not seek to.
     */
    public InvalidSeekPositionException(ShardPosition position) {
        super("Invalid seek position " + position);
        this.position = position;
    }

    /**
     * @return shard position.
     */
    public ShardPosition getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "Cannot seek to position " + position;
    }
}
