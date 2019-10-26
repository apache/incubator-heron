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
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.operators.KeyByOperator;

/**
 * KeyByStreamlet represents a KVStreamlet that is the result of applying key and value extractors
 * on all elements.
 */
public class KeyByStreamlet<R, K, V> extends StreamletImpl<KeyValue<K, V>> {
  private StreamletImpl<R> parent;
  private SerializableFunction<R, K> keyExtractor;
  private SerializableFunction<R, V> valueExtractor;

  public KeyByStreamlet(StreamletImpl<R> parent,
                        SerializableFunction<R, K> keyExtractor,
                        SerializableFunction<R, V> valueExtractor) {
    this.parent = parent;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.KEYBY, stageNames);
    KeyByOperator<R, K, V> bolt = new KeyByOperator<>(keyExtractor, valueExtractor);
    bldr.setBolt(getName(), bolt, getNumPartitions())
        .shuffleGrouping(parent.getName(), parent.getStreamId());
    return true;
  }
}
