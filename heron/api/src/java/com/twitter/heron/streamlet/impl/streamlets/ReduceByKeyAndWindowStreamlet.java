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
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBinaryOperator;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.StreamletImpl;
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
public class ReduceByKeyAndWindowStreamlet<K, V, R>
    extends StreamletImpl<KeyValue<KeyedWindow<K>, V>> {
  private StreamletImpl<R> parent;
  private SerializableFunction<R, K> keyExtractor;
  private SerializableFunction<R, V> valueExtractor;
  private WindowConfigImpl windowCfg;
  private SerializableBinaryOperator<V> reduceFn;

  public ReduceByKeyAndWindowStreamlet(StreamletImpl<R> parent,
                       SerializableFunction<R, K> keyExtractor,
                       SerializableFunction<R, V> valueExtractor,
                       WindowConfig windowCfg,
                       SerializableBinaryOperator<V> reduceFn) {
    this.parent = parent;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefixes.REDUCE.toString(), stageNames);
    ReduceByKeyAndWindowOperator<K, V, R> bolt = new ReduceByKeyAndWindowOperator<>(keyExtractor,
        valueExtractor, reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(),
            new ReduceByKeyAndWindowCustomGrouping<K, R>(keyExtractor));
    return true;
  }
}
