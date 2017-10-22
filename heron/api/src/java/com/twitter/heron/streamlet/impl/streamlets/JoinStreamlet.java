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
import com.twitter.heron.streamlet.impl.BaseKVStreamlet;
import com.twitter.heron.streamlet.impl.WindowConfigImpl;
import com.twitter.heron.streamlet.impl.groupings.JoinCustomGrouping;
import com.twitter.heron.streamlet.impl.operators.JoinOperator;

/**
 * JoinStreamlet represents a KVStreamlet that is the result of joining two KVStreamlets left
 * and right using a WindowConfig. For all left and right tuples in the window whose keys
 * match, the user supplied joinFunction is applied on the values to get the resulting value.
 * JoinStreamlet's elements are of KeyValue type where the key is KeyWindowInfo<K> type
 * and the value is of type VR.
 */
public final class JoinStreamlet<K, V1, V2, VR> extends BaseKVStreamlet<KeyedWindow<K>, VR> {
  private JoinOperator.JoinType joinType;
  private BaseKVStreamlet<K, V1> left;
  private BaseKVStreamlet<K, V2> right;
  private WindowConfigImpl windowCfg;
  private SerializableBiFunction<? super V1, ? super V2, ? extends VR> joinFn;

  public static <A, B, C, D> JoinStreamlet<A, B, C, D>
      createJoinStreamlet(BaseKVStreamlet<A, B> left,
                          BaseKVStreamlet<A, C> right,
                          WindowConfig windowCfg,
                          JoinOperator.JoinType joinType,
                          SerializableBiFunction<? super B, ? super C, ? extends D> joinFn) {
    return new JoinStreamlet<>(joinType, left,
        right, windowCfg, joinFn);
  }

  private JoinStreamlet(JoinOperator.JoinType joinType, BaseKVStreamlet<K, V1> left,
                        BaseKVStreamlet<K, V2> right,
                        WindowConfig windowCfg,
                        SerializableBiFunction<? super V1, ? super V2, ? extends VR> joinFn) {
    this.joinType = joinType;
    this.left = left;
    this.right = right;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.joinFn = joinFn;
    setNumPartitions(left.getNumPartitions());
  }

  public JoinOperator.JoinType getJoinType() {
    return joinType;
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (!left.isBuilt() || !right.isBuilt()) {
      return false;
    }
    if (getName() == null) {
      setName(defaultNameCalculator("join", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    JoinOperator<K, V1, V2, VR> bolt = new JoinOperator<>(joinType, left.getName(),
        right.getName(), joinFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(left.getName(), new JoinCustomGrouping<K, V1>())
        .customGrouping(right.getName(), new JoinCustomGrouping<K, V2>());
    return true;
  }
}
