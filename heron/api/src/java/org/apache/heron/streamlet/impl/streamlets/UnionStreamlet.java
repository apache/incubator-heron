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
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.operators.UnionOperator;

/**
 * UnionStreamlet is a Streamlet composed of all the elements of two
 * parent streamlets.
 */
public class UnionStreamlet<I> extends StreamletImpl<I> {
  private StreamletImpl<I> left;
  private StreamletImpl<? extends I> right;

  public UnionStreamlet(StreamletImpl<I> left, StreamletImpl<? extends I> right) {
    this.left = left;
    this.right = right;
    setNumPartitions(left.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (!left.isBuilt() || !right.isBuilt()) {
      // We can only continue to build us if both of our parents are built.
      // The system will call us again later
      return false;
    }
    setDefaultNameIfNone(StreamletNamePrefix.UNION, stageNames);
    bldr.setBolt(getName(), new UnionOperator<I>(),
        getNumPartitions())
          .shuffleGrouping(left.getName(), left.getStreamId())
          .shuffleGrouping(right.getName(), right.getStreamId());
    return true;
  }
}
