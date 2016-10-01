/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme;

/**
 * Sample scheme for emitting Kinesis records as tuples. It emits a tuple of (partitionKey, sequenceNumber, and data).
 */
public class SampleKinesisRecordScheme implements IKinesisRecordScheme {
    private static final long serialVersionUID = 1L;
    /**
     * Name of the (partition key) value in the tuple.
     */
    public static final String FIELD_PARTITION_KEY = "partitionKey";
    
    /**
     * Name of the sequence number value in the tuple.
     */
    public static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";
    
    /**
     * Name of the Kinesis record data value in the tuple.
     */
    public static final String FIELD_RECORD_DATA = "recordData";

    /**
     * Constructor.
     */
    public SampleKinesisRecordScheme() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#deserialize(com.amazonaws.services.kinesis.model
     * .Record)
     */
    @Override
    public List<Object> deserialize(Record record) {
        final List<Object> l = new ArrayList<>();
        l.add(record.getPartitionKey());
        l.add(record.getSequenceNumber());
        l.add(record.getData().array());
        return l;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#getOutputFields()
     */
    @Override
    public Fields getOutputFields() {
        return new Fields(FIELD_PARTITION_KEY, FIELD_SEQUENCE_NUMBER, FIELD_RECORD_DATA);
    }
}
