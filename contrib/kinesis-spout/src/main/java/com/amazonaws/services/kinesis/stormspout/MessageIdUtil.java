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
 * Helper class to convert (shardId, sequenceNumber) <-> messageId.
 */
class MessageIdUtil {

    private MessageIdUtil() {
    }

    /**
     * @param messageId MessageId of the tuple.
     * @return shardId of the record (tuple).
     */
    static String shardIdOfMessageId(String messageId) {
        return messageId.split(":")[0];
    }

    /**
     * @param messageId MessageId.
     * @return sequence number for the record (tuple)
     */
    static String sequenceNumberOfMessageId(String messageId) {
        return messageId.split(":")[1];
    }

    /**
     * Used to construct a messageId for a tuple corresponding to a Kinesis record.
     * @param shardId Shard from which the record was fetched.
     * @param sequenceNumber Sequence number of the record.
     * @return messageId
     */
    static Object constructMessageId(String shardId, String sequenceNumber) {
        String messageId = shardId + ":" + sequenceNumber;
        return messageId;
    }

}
