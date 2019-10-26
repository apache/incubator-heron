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
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.operators.FlatMapOperator;

/**
 * FlatMapStreamlet represents a Streamlet that is made up of applying the user
 * supplied flatMap function to each element of the parent streamlet and flattening
 * out the result.
 */
public class FlatMapStreamlet<R, T> extends StreamletImpl<T> {
  private StreamletImpl<R> parent;
  private SerializableFunction<? super R, ? extends Iterable<? extends T>> flatMapFn;

  public FlatMapStreamlet(StreamletImpl<R> parent,
                          SerializableFunction<? super R,
                                               ? extends Iterable<? extends T>> flatMapFn) {
    this.parent = parent;
    this.flatMapFn = flatMapFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.FLATMAP, stageNames);
    bldr.setBolt(getName(), new FlatMapOperator<R, T>(flatMapFn),
        getNumPartitions()).shuffleGrouping(parent.getName(), parent.getStreamId());
    return true;
  }
}
