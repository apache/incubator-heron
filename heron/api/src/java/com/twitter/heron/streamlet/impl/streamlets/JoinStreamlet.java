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
import com.twitter.heron.streamlet.JoinType;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.StreamletImpl;
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
public final class JoinStreamlet<K, R, S, T> extends StreamletImpl<KeyValue<KeyedWindow<K>, T>> {
  private JoinType joinType;
  private StreamletImpl<R> left;
  private StreamletImpl<S> right;
  private SerializableFunction<R, K> leftKeyExtractor;
  private SerializableFunction<S, K> rightKeyExtractor;
  private WindowConfigImpl windowCfg;
  private SerializableBiFunction<R, S, ? extends T> joinFn;
  private static final String NAMEPREFIX = "join";

  public static <A, B, C, D> JoinStreamlet<A, B, C, D>
      createJoinStreamlet(StreamletImpl<B> left,
                          StreamletImpl<C> right,
                          SerializableFunction<B, A> leftKeyExtractor,
                          SerializableFunction<C, A> rightKeyExtractor,
                          WindowConfig windowCfg,
                          JoinType joinType,
                          SerializableBiFunction<B, C, ? extends D> joinFn) {
    return new JoinStreamlet<>(joinType, left,
        right, leftKeyExtractor, rightKeyExtractor, windowCfg, joinFn);
  }

  private JoinStreamlet(JoinType joinType, StreamletImpl<R> left,
                        StreamletImpl<S> right,
                        SerializableFunction<R, K> leftKeyExtractor,
                        SerializableFunction<S, K> rightKeyExtractor,
                        WindowConfig windowCfg,
                        SerializableBiFunction<R, S, ? extends T> joinFn) {
    this.joinType = joinType;
    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.joinFn = joinFn;
    setNumPartitions(left.getNumPartitions());
  }

  public JoinType getJoinType() {
    return joinType;
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (!left.isBuilt() || !right.isBuilt()) {
      return false;
    }
    setDefaultNameIfNone(StreamletNamePrefixes.JOIN.toString(), stageNames);
    stageNames.add(getName());
    JoinOperator<K, R, S, T> bolt = new JoinOperator<>(joinType, left.getName(),
        right.getName(), leftKeyExtractor, rightKeyExtractor, joinFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(left.getName(), new JoinCustomGrouping<K, R>(leftKeyExtractor))
        .customGrouping(right.getName(), new JoinCustomGrouping<K, S>(rightKeyExtractor));
    return true;
  }
}
