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

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.Window;

/**
 * ReduceByKeyAndWindowOperator is the class that implements reduceByKeyAndWindow functionality.
 * It takes in a reduceFunction Function as an input.
 * For every time window, the bolt goes over all the tuples in that window and applies the reduce
 * function grouped by keys. It emits a KeyedWindow, reduced Value KeyPairs as outputs
 * R: Incoming data type, K: Key type, T: Result data type
 */
public class GeneralReduceByKeyAndWindowOperator<R, K, T>
    extends StreamletWindowOperator<R, KeyValue<KeyedWindow<K>, T>> {

  private SerializableFunction<R, K> keyExtractor;
  private T identity;
  private SerializableBiFunction<T, R, ? extends T> reduceFn;

  public GeneralReduceByKeyAndWindowOperator(SerializableFunction<R, K> keyExtractor,
                                             T identity,
                                             SerializableBiFunction<T, R, ? extends T> reduceFn) {
    this.keyExtractor = keyExtractor;
    this.identity = identity;
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(TupleWindow inputWindow) {
    Map<K, T> reduceMap = new HashMap<>();
    Map<K, Integer> windowCountMap = new HashMap<>();
    for (Tuple tuple : inputWindow.get()) {
      R tup = (R) tuple.getValue(0);
      addMap(reduceMap, windowCountMap, tup);
    }
    long startWindow;
    long endWindow;
    if (inputWindow.getStartTimestamp() == null) {
      startWindow = 0;
    } else {
      startWindow = inputWindow.getStartTimestamp();
    }
    if (inputWindow.getEndTimestamp() == null) {
      endWindow = 0;
    } else {
      endWindow = inputWindow.getEndTimestamp();
    }
    for (K key : reduceMap.keySet()) {
      Window window = new Window(startWindow, endWindow, windowCountMap.get(key));
      KeyedWindow<K> keyedWindow = new KeyedWindow<>(key, window);
      collector.emit(new Values(new KeyValue<>(keyedWindow, reduceMap.get(key))));
    }
  }

  private void addMap(Map<K, T> reduceMap, Map<K, Integer> windowCountMap, R tup) {
    K key = keyExtractor.apply(tup);
    if (!reduceMap.containsKey(key)) {
      reduceMap.put(key, identity);
      windowCountMap.put(key, 0);
    }
    reduceMap.put(key, reduceFn.apply(reduceMap.get(key), tup));
    windowCountMap.put(key, windowCountMap.get(key) + 1);
  }
}
