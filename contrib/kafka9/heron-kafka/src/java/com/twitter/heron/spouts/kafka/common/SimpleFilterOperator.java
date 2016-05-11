/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/* This is a simple filter operator that samples with uniformly random probability of
 * sampleFreq.
 */
public class SimpleFilterOperator extends FilterOperator {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFilterOperator.class);

    private double sampleFreq;
    private Random random;

    /**
     * Ctor
     */
    public SimpleFilterOperator(String sampleFreq) {
        super(sampleFreq);
        try {
            this.sampleFreq = Double.parseDouble(sampleFreq);
        } catch (NumberFormatException e) {
            LOG.error("Invalid sample frequency " + sampleFreq, e);
        }
        if (this.sampleFreq > 1) {
            // Deprecated. Present for legacy reason, to provide one out of N message.
            this.sampleFreq = 1 / (this.sampleFreq);
        }
        random = new Random();
    }

    @Override
    public boolean filter(byte[] tuple, Long kafkaLag) {
        return random.nextDouble() > sampleFreq;
    }
}
