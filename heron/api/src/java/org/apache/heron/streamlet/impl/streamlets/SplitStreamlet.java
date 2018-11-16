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

import java.util.List;
import java.util.Set;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.operators.SplitOperator;

/**
 * SplitStreamlet represents a Streamlet that splits an incoming
 * stream into multiple streams using a split function. Each tuple
 * can be emitted into no or multiple streams.
 */
public class SplitStreamlet<R> extends StreamletImpl<R> {
  private StreamletImpl<R> parent;
  private String parentStreamId;
  private SerializableFunction<? super R, List<String>> splitFn;

  public SplitStreamlet(StreamletImpl<R> parent,
                        SerializableFunction<? super R, List<String>> splitFn) {
    this.parent = parent;
    this.parentStreamId = parent.getStreamId();
    this.splitFn = splitFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.SPLIT, stageNames);
    bldr.setBolt(getName(), new SplitOperator<R>(splitFn),
        getNumPartitions()).shuffleGrouping(parent.getName(), parentStreamId);
    return true;
  }
}
