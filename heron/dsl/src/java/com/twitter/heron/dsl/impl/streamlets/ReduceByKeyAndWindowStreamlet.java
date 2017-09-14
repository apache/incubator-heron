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
import com.twitter.heron.dsl.KeyedWindow;
import com.twitter.heron.dsl.SerializableBinaryOperator;
import com.twitter.heron.dsl.WindowConfig;
import com.twitter.heron.dsl.impl.BaseKVStreamlet;
import com.twitter.heron.dsl.impl.WindowConfigImpl;
import com.twitter.heron.dsl.impl.groupings.ReduceByKeyAndWindowCustomGrouping;
import com.twitter.heron.dsl.impl.operators.ReduceByKeyAndWindowOperator;

/**
 * ReduceByKeyAndWindowStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements within each window defined by a
 * user supplied Window Config.
 * ReduceByKeyAndWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo<K> type and the value is of type V.
 */
public class ReduceByKeyAndWindowStreamlet<K, V> extends BaseKVStreamlet<KeyedWindow<K>, V> {
  private BaseKVStreamlet<K, V> parent;
  private WindowConfigImpl windowCfg;
  private SerializableBinaryOperator<V> reduceFn;

  public ReduceByKeyAndWindowStreamlet(BaseKVStreamlet<K, V> parent,
                       WindowConfig windowCfg,
                       SerializableBinaryOperator<V> reduceFn) {
    this.parent = parent;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("reduceByKeyAndWindow", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    ReduceByKeyAndWindowOperator<K, V> bolt = new ReduceByKeyAndWindowOperator<>(reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), new ReduceByKeyAndWindowCustomGrouping<K, V>());
    return true;
  }
}
