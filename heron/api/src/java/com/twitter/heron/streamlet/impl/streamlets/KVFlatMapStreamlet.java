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
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.impl.KVStreamletImpl;
import com.twitter.heron.streamlet.impl.operators.FlatMapOperator;

/**
 * FlatMapStreamlet represents a Streamlet that is made up of applying the user
 * supplied flatMap function to each element of the parent streamlet and flattening
 * out the result.
 */
public class KVFlatMapStreamlet<K, V, K1, V1> extends KVStreamletImpl<K1, V1> {
  private KVStreamletImpl<K, V> parent;
  private SerializableFunction<KeyValue<K, V>,
      ? extends Iterable<KeyValue<? extends K1, ? extends V1>>> flatMapFn;

  public KVFlatMapStreamlet(KVStreamletImpl<K, V> parent,
                            SerializableFunction<KeyValue<K, V>,
                              ? extends Iterable<KeyValue<? extends K1, ? extends V1>>> flatMapFn) {
    this.parent = parent;
    this.flatMapFn = flatMapFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("kvflatmap", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setBolt(getName(), new FlatMapOperator<KeyValue<K, V>,
            KeyValue<? extends K1, ? extends V1>>(flatMapFn),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
