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

/**
 * An interface that allows us to extract the timestamp associated with an emitted tuple.
 */
public interface TimestampExtractor {
    /**
     * Returns the timestamp associated with the given tuple.
     *
     * @param tuple The tuple.
     * @return The timestamp associated with the given tuple.
     * @throw ClassCastException If the tuple cannot be converted to a class that this timestamp
     * extractor can operate on.
     * @throw IllegalArgumentException If the timestamp cannot be extracted from the given tuple.
     */
    long getTimestamp(Object tuple) throws ClassCastException, IllegalArgumentException;
}
