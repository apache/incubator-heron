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
import com.twitter.heron.dsl.SerializableBinaryOperator;
import com.twitter.heron.dsl.Window;
import com.twitter.heron.dsl.WindowConfig;
import com.twitter.heron.dsl.impl.BaseKVStreamlet;
import com.twitter.heron.dsl.impl.BaseStreamlet;
import com.twitter.heron.dsl.impl.WindowConfigImpl;
import com.twitter.heron.dsl.impl.groupings.ReduceByWindowCustomGrouping;
import com.twitter.heron.dsl.impl.operators.ReduceByWindowOperator;

/**
 * ReduceByWindowStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements within each window defined by a
 * user supplied Window Config.
 * ReduceByWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo<K> type and the value is of type V.
 */
public class ReduceByWindowStreamlet<I> extends BaseKVStreamlet<Window, I> {
  private BaseStreamlet<I> parent;
  private WindowConfigImpl windowCfg;
  private SerializableBinaryOperator<I> reduceFn;

  public ReduceByWindowStreamlet(BaseStreamlet<I> parent,
                                 WindowConfig windowCfg,
                                 SerializableBinaryOperator<I> reduceFn) {
    this.parent = parent;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("reduceByWindow", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    ReduceByWindowOperator<I> bolt = new ReduceByWindowOperator<>(reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), new ReduceByWindowCustomGrouping<I>());
    return true;
  }
}
