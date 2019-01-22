/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.heron.streamlet.impl.streamlets;

import java.util.Set;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.groupings.ReduceByKeyAndWindowCustomGrouping;
import org.apache.heron.streamlet.impl.operators.GeneralReduceByKeyOperator;

/**
 * GeneralReduceByKeyStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements.
 * GeneralReduceByKeyStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo&lt;K&gt; type and the value is of type T.
 */
public class GeneralReduceByKeyStreamlet<R, K, T> extends StreamletImpl<KeyValue<K, T>> {
  private StreamletImpl<R> parent;
  private SerializableFunction<R, K> keyExtractor;
  private T identity;
  private SerializableBiFunction<T, R, ? extends T> reduceFn;

  public GeneralReduceByKeyStreamlet(StreamletImpl<R> parent,
                                     SerializableFunction<R, K> keyExtractor,
                                     T identity,
                                     SerializableBiFunction<T, R, ? extends T> reduceFn) {
    this.parent = parent;
    this.keyExtractor = keyExtractor;
    this.identity = identity;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.REDUCE, stageNames);
    GeneralReduceByKeyOperator<R, K, T> bolt =
        new GeneralReduceByKeyOperator<R, K, T>(keyExtractor, identity, reduceFn);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), parent.getStreamId(),
            new ReduceByKeyAndWindowCustomGrouping<R, K>(keyExtractor));
    return true;
  }
}
