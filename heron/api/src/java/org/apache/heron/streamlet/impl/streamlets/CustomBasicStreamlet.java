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
import org.apache.heron.streamlet.IStreamletBasicOperator;
import org.apache.heron.streamlet.impl.StreamletImpl;

/**
 * CustomBasicStreamlet represents a Streamlet that is made up of applying the user
 * supplied custom operator to each element of the parent streamlet.
 */
public class CustomBasicStreamlet<R, T> extends StreamletImpl<T> {
  private StreamletImpl<R> parent;
  private IStreamletBasicOperator<R, T> op;

  /**
   * Create a custom streamlet from user defined CustomBasicOperator object.
   * @param parent The parent(upstream) streamlet object
   * @param op The user defined CustomeBasicOperator
   */
  public CustomBasicStreamlet(StreamletImpl<R> parent,
                              IStreamletBasicOperator<R, T> op) {
    this.parent = parent;
    this.op = op;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.CUSTOM_BASIC, stageNames);
    bldr.setBolt(getName(), op,  getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
