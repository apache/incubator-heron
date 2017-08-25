// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.dsl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.dsl.windowing.WindowConfig;

/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
 csv files, HDFS files), or for that matter any other source. They are also created by
 transforming existing Streamlets using operations such as map/flatMap, etc.
 Besides the tuples, a Streamlet has the following properties associated with it
 a) name. User assigned or system generated name to refer the streamlet
 b) nPartitions. Number of partitions that the streamlet is composed of. The nPartitions
 could be assigned by the user or computed by the system
 */
class ReduceByKeyAndWindowBolt<K, V> extends DslBolt, WindowBolt {
  private static final long serialVersionUID = 2833576046687750496L;
  private WindowConfig windowCfg;
  private BinaryOperator<V> reduceFn;
  private OutputCollector collector;

  ReduceByKeyAndWindowBolt(WindowConfig windowCfg, BinaryOperator<V> reduceFn) {
    this.windowCfg = windowCfg;
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleWindow(List<Tuple> tuples) {
    Map<K, V> reduceMap = new HashMap<>();
    for (Tuple tuple : tuples) {
      KeyValue<K, V> tup = (KeyValue<K, V>) tuple.getValue(0);
      addMap(reduceMap, tup);
    }
    for (K key : reduceMap.keySet()) {
      collector.emit(new Values(new KeyValue<K, V>(key, reduceMap.get(key))));
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
