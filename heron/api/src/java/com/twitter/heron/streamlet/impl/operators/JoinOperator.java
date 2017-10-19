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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.Pair;
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
 * JoinOperator is the bolt that implements the join/leftJoin/innerJoin functionality.
 * It embeds the logic of the type of join(outer, left, inner) which it takes in as
 * a config parameter. Also taken as parameters are which source is left and right.
 * This is needed for the semantics of outer/left/inner joins.
 */
public class JoinOperator<K, V1, V2, VR> extends StreamletWindowOperator {
  private static final long serialVersionUID = 4875450390444745407L;
  public static final String LEFT_COMPONENT_NAME = "_streamlet_joinbolt_left_component_name_";
  public static final String RIGHT_COMPONENT_NAME = "_streamlet_joinbolt_right_component_name_";

  public enum JoinType {
    INNER,
    OUTER_LEFT,
    OUTER_RIGHT;
  }

  private JoinType joinType;
  // The source component that represent the left join component
  private String leftComponent;
  // The source component that represent the right join component
  private String rightComponent;
  // The user supplied join function
  private SerializableBiFunction<? super V1, ? super V2, ? extends VR> joinFn;
  private OutputCollector collector;

  public JoinOperator(JoinType joinType, String leftComponent, String rightComponent,
                      SerializableBiFunction<? super V1, ? super V2, ? extends VR> joinFn) {
    this.joinType = joinType;
    this.leftComponent = leftComponent;
    this.rightComponent = rightComponent;
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
    Map<K, Pair<List<V1>, List<V2>>> joinMap = new HashMap<>();
    for (Tuple tuple : inputWindow.get()) {
      if (tuple.getSourceComponent().equals(leftComponent)) {
        KeyValue<K, V1> tup = (KeyValue<K, V1>) tuple.getValue(0);
        if (tup.getKey() != null) {
          addMapLeft(joinMap, tup);
        }
      } else {
        KeyValue<K, V2> tup = (KeyValue<K, V2>) tuple.getValue(0);
        if (tup.getKey() != null) {
          addMapRight(joinMap, tup);
        }
      }
    }
    evaluateJoinMap(joinMap, inputWindow);
  }

  private void evaluateJoinMap(Map<K, Pair<List<V1>, List<V2>>> joinMap, TupleWindow tupleWindow) {
    for (K key : joinMap.keySet()) {
      Pair<List<V1>, List<V2>> val = joinMap.get(key);
      switch (joinType) {
        case INNER:
          if (!val.getFirst().isEmpty() && !val.getSecond().isEmpty()) {
            innerJoinAndEmit(key, tupleWindow, val);
          }
          break;
        case OUTER_LEFT:
          if (!val.getFirst().isEmpty() && !val.getSecond().isEmpty()) {
            innerJoinAndEmit(key, tupleWindow, val);
          } else if (!val.getFirst().isEmpty()) {
            outerLeftJoinAndEmit(key, tupleWindow, val);
          }
          break;
        case OUTER_RIGHT:
          if (!val.getFirst().isEmpty() && !val.getSecond().isEmpty()) {
            innerJoinAndEmit(key, tupleWindow, val);
          } else if (!val.getSecond().isEmpty()) {
            outerRightJoinAndEmit(key, tupleWindow, val);
          }
          break;
        default:
          throw new RuntimeException("Unknown join type " + joinType.name());
      }
    }
  }

  private void addMapLeft(Map<K, Pair<List<V1>, List<V2>>> joinMap, KeyValue<K, V1> tup) {
    if (!joinMap.containsKey(tup.getKey())) {
      joinMap.put(tup.getKey(), Pair.of(new LinkedList<>(), new LinkedList<>()));
    }
    joinMap.get(tup.getKey()).getFirst().add(tup.getValue());
  }

  private void addMapRight(Map<K, Pair<List<V1>, List<V2>>> joinMap, KeyValue<K, V2> tup) {
    if (!joinMap.containsKey(tup.getKey())) {
      joinMap.put(tup.getKey(), Pair.of(new LinkedList<>(), new LinkedList<>()));
    }
    joinMap.get(tup.getKey()).getSecond().add(tup.getValue());
  }

  private KeyedWindow<K> getKeyedWindow(K key, TupleWindow tupleWindow) {
    long startWindow;
    long endWindow;
    if (tupleWindow.getStartTimestamp() == null) {
      startWindow = 0;
    } else {
      startWindow = tupleWindow.getStartTimestamp();
    }
    if (tupleWindow.getEndTimestamp() == null) {
      endWindow = 0;
    } else {
      endWindow = tupleWindow.getEndTimestamp();
    }
    Window window = new Window(startWindow, endWindow, tupleWindow.get().size());
    return new KeyedWindow<>(key, window);
  }

  private void innerJoinAndEmit(K key, TupleWindow tupleWindow, Pair<List<V1>, List<V2>> val) {
    KeyedWindow<K> keyedWindow = getKeyedWindow(key, tupleWindow);
    for (V1 val1 : val.getFirst()) {
      for (V2 val2 : val.getSecond()) {
        collector.emit(new Values(new KeyValue<>(keyedWindow,
            joinFn.apply(val1, val2))));
      }
    }
  }

  private void outerLeftJoinAndEmit(K key, TupleWindow tupleWindow, Pair<List<V1>, List<V2>> val) {
    KeyedWindow<K> keyedWindow = getKeyedWindow(key, tupleWindow);
    for (V1 val1 : val.getFirst()) {
      collector.emit(new Values(new KeyValue<>(keyedWindow,
          joinFn.apply(val1, null))));
    }
  }

  private void outerRightJoinAndEmit(K key, TupleWindow tupleWindow, Pair<List<V1>, List<V2>> val) {
    KeyedWindow<K> keyedWindow = getKeyedWindow(key, tupleWindow);
    for (V2 val2 : val.getSecond()) {
      collector.emit(new Values(new KeyValue<>(keyedWindow,
          joinFn.apply(null, val2))));
    }
  }
}
