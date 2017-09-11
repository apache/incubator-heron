//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.dsl.impl.operators;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.KeyedWindowInfo;
import com.twitter.heron.dsl.SerializableBinaryOperator;
import com.twitter.heron.dsl.WindowInfo;

/**
 * ReduceByKeyAndWindowBolt is the class that implements reduceByKeyAndWindow functionality.
 * It takes in a reduceFunction Function as an input.
 * For every time window, the bolt goes over all the tuples in that window and applies the reduce
 * function grouped by keys. It emits a KeyedWindowInfo, reduced Value KeyPairs as outputs
 */
public class ReduceByKeyAndWindowBolt<K, V> extends DslWindowBolt {
  private static final long serialVersionUID = 2833576046687750496L;
  private SerializableBinaryOperator<V> reduceFn;
  private OutputCollector collector;

  public ReduceByKeyAndWindowBolt(SerializableBinaryOperator<V> reduceFn) {
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(TupleWindow inputWindow) {
    Map<K, V> reduceMap = new HashMap<>();
    for (Tuple tuple : inputWindow.get()) {
      KeyValue<K, V> tup = (KeyValue<K, V>) tuple.getValue(0);
      addMap(reduceMap, tup);
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
    WindowInfo windowInfo = new WindowInfo(startWindow, endWindow);
    for (K key : reduceMap.keySet()) {
      KeyedWindowInfo<K> keyedWindowInfo = new KeyedWindowInfo<>(key, windowInfo);
      collector.emit(new Values(new KeyValue<>(keyedWindowInfo, reduceMap.get(key))));
    }
  }

  private void addMap(Map<K, V> reduceMap, KeyValue<K, V> tup) {
    if (reduceMap.containsKey(tup.getKey())) {
      reduceMap.put(tup.getKey(), reduceFn.apply(reduceMap.get(tup.getKey()), tup.getValue()));
    } else {
      reduceMap.put(tup.getKey(), tup.getValue());
    }
  }
}
