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

import java.util.List;

import backtype.storm.tuple.Fields;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Used to convert Kinesis record into a tuple.
 */
public interface IKinesisRecordScheme extends java.io.Serializable {

    /**
     * @param record Kinesis record
     * @return List of values (to be emitted as a tuple)
     */
    List<Object> deserialize(Record record);

    /**
     * @return output fields
     */
    Fields getOutputFields();

}
