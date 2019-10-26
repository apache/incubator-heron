/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.streamlet.impl.operators;

import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.SerializableFunction;

/**
 * KeyByOperator is the class that implements keyBy functionality.
 * It takes in a key extractor and a value extractor as input.
 * For every tuple, the bolt convert the stream to a key-value pair tuple
 * by applying the extractors.
 */
public class KeyByOperator<R, K, V> extends StreamletOperator<R, KeyValue<K, V>> {
  private SerializableFunction<R, K> keyExtractor;
  private SerializableFunction<R, V> valueExtractor;

  public KeyByOperator(SerializableFunction<R, K> keyExtractor,
                       SerializableFunction<R, V> valueExtractor) {
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    K key = keyExtractor.apply(obj);
    V value = valueExtractor.apply(obj);

    collector.emit(new Values(new KeyValue<>(key, value)));
    collector.ack(tuple);
  }
}
