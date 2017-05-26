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

/**
 * Thrown when we encounter issues fetching data from Kinesis, storing state, or emitting tuples.
 */
public class KinesisSpoutException extends RuntimeException {
    private static final long serialVersionUID = 140102478556038072L;

    /** Constructor.
     * @param message Message with details.
     * @param cause Cause
     */
    public KinesisSpoutException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Constructor.
     * @param message Message with details.
     */
    public KinesisSpoutException(String message) {
        super(message);
    }

    /** Constructor.
     * @param cause Cause of the exception.
     */
    public KinesisSpoutException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     */
    public KinesisSpoutException() {
        super("Encountered exception in KinesisSpout");
    }
}
