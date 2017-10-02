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
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.SerializableFunction;
import com.twitter.heron.dsl.impl.BaseKVStreamlet;
import com.twitter.heron.dsl.impl.BaseStreamlet;

/**
 * KVFlatMapStreamlet represents a KVStreamlet that is made up of applying the user
 * supplied flatMap function to each element of the parent streamlet and flattening
 * out the result. The only difference between this and FlatMapStreamlet is that
 * KVFlatMapStreamlet ensures that resulting elements are of type KeyValue.
 */
public class KVFlatMapStreamlet<R, K, V> extends BaseKVStreamlet<K, V> {
  private FlatMapStreamlet<? super R, ? extends KeyValue<K, V>> delegate;

  public KVFlatMapStreamlet(BaseStreamlet<R> parent,
                            SerializableFunction<? super R,
                                                 Iterable<? extends KeyValue<K, V>>> flatMapFn) {
    this.delegate = new FlatMapStreamlet<>(parent, flatMapFn);
    setNumPartitions(delegate.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    boolean retval = this.delegate.doBuild(bldr, stageNames);
    setName(delegate.getName());
    return retval;
  }
}
