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

package com.twitter.heron.dsl.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.dsl.KeyedWindowInfo;
import com.twitter.heron.dsl.SerializableBinaryOperator;
import com.twitter.heron.dsl.WindowConfig;
import com.twitter.heron.dsl.impl.KVStreamletImpl;
import com.twitter.heron.dsl.impl.WindowConfigImpl;
import com.twitter.heron.dsl.impl.bolts.ReduceByKeyAndWindowBolt;
import com.twitter.heron.dsl.impl.groupings.ReduceByKeyAndWindowCustomGrouping;

/**
 * ReduceByKeyAndWindowStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements within each window defined by a
 * user supplied Window Config.
 * ReduceByKeyAndWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo<K> type and the value is of type V.
 */
public class ReduceByKeyAndWindowStreamlet<K, V> extends KVStreamletImpl<KeyedWindowInfo<K>, V> {
  private KVStreamletImpl<K, V> parent;
  private WindowConfigImpl windowCfg;
  private SerializableBinaryOperator<V> reduceFn;

  public ReduceByKeyAndWindowStreamlet(KVStreamletImpl<K, V> parent,
                       WindowConfig windowCfg,
                       SerializableBinaryOperator<V> reduceFn) {
    this.parent = parent;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  private void calculateName(Set<String> stageNames) {
    int index = 1;
    String name;
    while (true) {
      name = new StringBuilder("reduceByKeyAndWindow").append(index).toString();
      if (!stageNames.contains(name)) {
        break;
      }
      index++;
    }
    setName(name);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      calculateName(stageNames);
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    ReduceByKeyAndWindowBolt<K, V> bolt = new ReduceByKeyAndWindowBolt<>(reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), new ReduceByKeyAndWindowCustomGrouping<K, V>());
    return true;
  }
}
