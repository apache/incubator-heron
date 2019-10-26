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
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.StreamletReducers;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.groupings.ReduceByKeyAndWindowCustomGrouping;
import org.apache.heron.streamlet.impl.operators.ReduceByKeyAndWindowOperator;

/**
 * CountByKeyAndWindowStreamlet represents a KVStreamlet that is the result of
 * counting all elements within each window defined by a user supplied Window Config.
 * ReduceByKeyAndWindowStreamlet's elements are of KeyValue type where the key is
 * KeyWindowInfo&lt;K&gt; type and the value is Long.
 */
public class CountByKeyAndWindowStreamlet<R, K>
    extends StreamletImpl<KeyValue<KeyedWindow<K>, Long>> {
  private StreamletImpl<R> parent;
  private SerializableFunction<R, K> keyExtractor;
  private WindowConfig windowCfg;

  public CountByKeyAndWindowStreamlet(StreamletImpl<R> parent,
                                      SerializableFunction<R, K> keyExtractor,
                                      WindowConfig windowCfg) {
    this.parent = parent;
    this.keyExtractor = keyExtractor;
    this.windowCfg = windowCfg;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.COUNT, stageNames);
    // Count is a special case of reduce operation. Hence ReduceByKeyAndWindowOperator
    // is used here. Every tuple has a value of 1 and the reduce operation is a simple sum.
    ReduceByKeyAndWindowOperator<R, K, Long> bolt =
        new ReduceByKeyAndWindowOperator<R, K, Long>(keyExtractor, x -> 1L, StreamletReducers::sum);
    windowCfg.applyTo(bolt);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .customGrouping(parent.getName(), parent.getStreamId(),
            // TODO: rename ReduceByKeyAndWindowCustomGrouping
            new ReduceByKeyAndWindowCustomGrouping<R, K>(keyExtractor));
    return true;
  }
}
