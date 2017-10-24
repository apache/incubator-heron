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

import java.util.List;
import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.impl.KVStreamletImpl;
import com.twitter.heron.streamlet.impl.groupings.RemapCustomGrouping;
import com.twitter.heron.streamlet.impl.operators.MapOperator;

/**
 * RemapStreamlet represents a Streamlet that is the result of
 * applying user supplied remapFn on all elements of its parent Streamlet.
 * RemapStreamlet as such is a generalized version of the Map/FlatMapStreamlets
 * that give users more flexibility over the operation. The remapFn allows for
 * users to choose which destination shards every transformed element can go.
 */
public class KVRemapStreamlet<K, V> extends KVStreamletImpl<K, V> {
  private KVStreamletImpl<K, V> parent;
  private SerializableBiFunction<? super KeyValue<K, V>, Integer, List<Integer>> remapFn;

  public KVRemapStreamlet(KVStreamletImpl<K, V> parent,
                          SerializableBiFunction<? super KeyValue<K, V>,
                              Integer, List<Integer>> remapFn) {
    this.parent = parent;
    this.remapFn = remapFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("kvremap", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setBolt(getName(), new MapOperator<KeyValue<K, V>, KeyValue<K, V>>((a) -> a),
        getNumPartitions())
        .customGrouping(parent.getName(), new RemapCustomGrouping<KeyValue<K, V>>(remapFn));
    return true;
  }
}
