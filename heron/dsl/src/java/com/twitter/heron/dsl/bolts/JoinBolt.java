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

package com.twitter.heron.dsl.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.dsl.KeyValue;
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
public class JoinBolt<K, V1, V2, VR> extends DslWindowBolt {
  private static final long serialVersionUID = 4875450390444745407L;
  public static final String LEFT_COMPONENT_NAME = "_dsl_joinbolt_left_component_name_";
  public static final String RIGHT_COMPONENT_NAME = "_dsl_joinbolt_right_component_name_";
  private String leftComponent;
  private String rightComponent;
  private WindowConfig windowCfg;
  private BiFunction<V1, V2, VR> joinFn;
  private OutputCollector collector;

  public JoinBolt(String leftComponent, String rightComponent, WindowConfig windowCfg,
           BiFunction<V1, V2, VR> joinFn) {
    this.leftComponent = leftComponent;
    this.rightComponent = rightComponent;
    this.windowCfg = windowCfg;
    this.joinFn = joinFn;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> cfg = super.getComponentConfiguration();
    cfg.put(LEFT_COMPONENT_NAME, leftComponent);
    cfg.put(RIGHT_COMPONENT_NAME, rightComponent);
    return cfg;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(TupleWindow inputWindow) {
    Map<K, KeyValue<V1, V2>> joinMap = new HashMap<>();
    for (Tuple tuple : inputWindow.get()) {
      if (tuple.getSourceComponent().equals(leftComponent)) {
        KeyValue<K, V1> tup = (KeyValue<K, V1>) tuple.getValue(0);
        addMapLeft(joinMap, tup);
      } else {
        KeyValue<K, V2> tup = (KeyValue<K, V2>) tuple.getValue(0);
        addMapRight(joinMap, tup);
      }
    }
    for (K key : joinMap.keySet()) {
      KeyValue<V1, V2> val = joinMap.get(key);
      KeyValue<K, VR> t = new KeyValue<K, VR>(key, joinFn.apply(val.getKey(), val.getValue()));
      collector.emit(new Values(t));
    }
  }

  private void addMapLeft(Map<K, KeyValue<V1, V2>> joinMap, KeyValue<K, V1> tup) {
    if (joinMap.containsKey(tup.getKey())) {
      joinMap.get(tup.getKey()).setKey(tup.getValue());
    } else {
      joinMap.put(tup.getKey(), new KeyValue<V1, V2>(tup.getValue(), null));
    }
  }

  private void addMapRight(Map<K, KeyValue<V1, V2>> joinMap, KeyValue<K, V2> tup) {
    if (joinMap.containsKey(tup.getKey())) {
      joinMap.get(tup.getKey()).setValue(tup.getValue());
    } else {
      joinMap.put(tup.getKey(), new KeyValue<V1, V2>(null, tup.getValue()));
    }
  }
}
