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

package com.twitter.heron.spouts.kafka;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Random;

/* This is a filter operator that tries to avoid lag. User can specify a minLag and a maxLag.
 * Sampling will kick in after lag become over minLag. And then sampling frequency increase
 * linearly as lag increases from minLag to maxLag. Once lag is greater than maxLag, everything
 * will be filtered. Instead if only maxLag and a sampleRate is defined, then once lag increases
 * beyond maxLag, then tuples are filtered uniformly by sampleRate.
 */
public class LagAvoidanceFilterOperator extends FilterOperator {
    private long maxLag;
    private long minLag;
    private final Random random;

    /**
     * Sets adaptive sampling rate
     *
     * @param parameter JSon encoded value of maxLag and sampleRate or minLag.
     */
    public LagAvoidanceFilterOperator(String parameter) {
        super(parameter);
        JSONObject json = (JSONObject) JSONValue.parse(parameter);
        this.maxLag = Integer.parseInt(json.get("maxLag").toString());
        this.minLag = Integer.parseInt(json.get("minLag").toString());
        random = new Random();
    }

    @Override
    public boolean filter(byte[] tuple, Long kafkaLag) {
        double rate = 0;
        if (kafkaLag == null) {
            return false;
        }
        if (minLag != maxLag) {
            rate = (kafkaLag - minLag) / (double) (maxLag - minLag);
        } else {
            // If minLag and maxLag are equal, It implies everything is to be filtered.
            rate = 1.0;
        }
        return (kafkaLag > minLag) && (random.nextDouble() < rate);
    }
}
