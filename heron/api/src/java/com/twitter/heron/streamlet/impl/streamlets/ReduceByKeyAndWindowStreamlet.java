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

package com.twitter.heron.streamlet.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.KVStreamletImpl;
import com.twitter.heron.streamlet.impl.WindowConfigImpl;
import com.twitter.heron.streamlet.impl.groupings.ReduceByKeyAndWindowCustomGrouping;
import com.twitter.heron.streamlet.impl.operators.ReduceByKeyAndWindowOperator;

/**
 * ReduceByKeyAndWindowStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements within each window defined by a
 * user supplied Window Config.
 * ReduceByKeyAndWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo<K> type and the value is of type V.
 */
public class ReduceByKeyAndWindowStreamlet<K, V, VR>
    extends KVStreamletImpl<KeyedWindow<K>, VR> {
  private KVStreamletImpl<K, V> parent;
  private WindowConfigImpl windowCfg;
  private VR identity;
  private SerializableBiFunction<? super VR, ? super V, ? extends VR> reduceFn;

  public ReduceByKeyAndWindowStreamlet(KVStreamletImpl<K, V> parent,
                            WindowConfig windowCfg,
                            VR identity,
                            SerializableBiFunction<? super VR, ? super V, ? extends VR> reduceFn) {
    this.parent = parent;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.identity = identity;
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
    ReduceByKeyAndWindowOperator<K, V, VR> bolt =
        new ReduceByKeyAndWindowOperator<K, V, VR>(identity, reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), new ReduceByKeyAndWindowCustomGrouping<K, V>());
    return true;
  }
}
