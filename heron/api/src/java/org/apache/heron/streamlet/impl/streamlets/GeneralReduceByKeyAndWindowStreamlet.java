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
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.WindowConfigImpl;
import org.apache.heron.streamlet.impl.groupings.ReduceByKeyAndWindowCustomGrouping;
import org.apache.heron.streamlet.impl.operators.GeneralReduceByKeyAndWindowOperator;

/**
 * ReduceByKeyAndWindowStreamlet represents a KVStreamlet that is the result of
 * applying user supplied reduceFn on all elements within each window defined by a
 * user supplied Window Config.
 * ReduceByKeyAndWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo<K> type and the value is of type V.
 */
public class GeneralReduceByKeyAndWindowStreamlet<K, V, VR>
    extends StreamletImpl<KeyValue<KeyedWindow<K>, VR>> {
  private StreamletImpl<V> parent;
  private SerializableFunction<V, K> keyExtractor;
  private WindowConfigImpl windowCfg;
  private VR identity;
  private SerializableBiFunction<VR, V, ? extends VR> reduceFn;

  public GeneralReduceByKeyAndWindowStreamlet(StreamletImpl<V> parent,
                            SerializableFunction<V, K> keyExtractor,
                            WindowConfig windowCfg,
                            VR identity,
                            SerializableBiFunction<VR, V, ? extends VR> reduceFn) {
    this.parent = parent;
    this.keyExtractor = keyExtractor;
    this.windowCfg = (WindowConfigImpl) windowCfg;
    this.identity = identity;
    this.reduceFn = reduceFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.REDUCE, stageNames);
    GeneralReduceByKeyAndWindowOperator<K, V, VR> bolt =
        new GeneralReduceByKeyAndWindowOperator<K, V, VR>(keyExtractor, identity, reduceFn);
    windowCfg.attachWindowConfig(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(),
            new ReduceByKeyAndWindowCustomGrouping<K, V>(keyExtractor));
    return true;
  }
}
