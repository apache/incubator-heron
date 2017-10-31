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

package com.twitter.heron.streamlet.impl.operators;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.Window;

/**
 * ReduceByKeyAndWindowOperator is the class that implements reduceByKeyAndWindow functionality.
 * It takes in a reduceFunction Function as an input.
 * For every time window, the bolt goes over all the tuples in that window and applies the reduce
 * function grouped by keys. It emits a KeyedWindow, reduced Value KeyPairs as outputs
 */
public class GeneralReduceByKeyAndWindowOperator<K, V, VR> extends StreamletWindowOperator {
  private static final long serialVersionUID = 2833576046687752396L;
  private VR identity;
  private SerializableBiFunction<? super VR, ? super V, ? extends VR> reduceFn;
  private OutputCollector collector;

  public GeneralReduceByKeyAndWindowOperator(VR identity,
                            SerializableBiFunction<? super VR, ? super V, ? extends VR> reduceFn) {
    this.identity = identity;
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
    Map<K, VR> reduceMap = new HashMap<>();
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
    Window window = new Window(startWindow, endWindow, inputWindow.get().size());
    for (K key : reduceMap.keySet()) {
      KeyedWindow<K> keyedWindow = new KeyedWindow<>(key, window);
      collector.emit(new Values(new KeyValue<>(keyedWindow, reduceMap.get(key))));
    }
  }

  private void addMap(Map<K, VR> reduceMap, KeyValue<K, V> tup) {
    if (!reduceMap.containsKey(tup.getKey())) {
      reduceMap.put(tup.getKey(), identity);
    }
    reduceMap.put(tup.getKey(), reduceFn.apply(reduceMap.get(tup.getKey()), tup.getValue()));
  }
}
