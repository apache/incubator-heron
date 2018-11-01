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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.api.topology.BoltDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.IStreamletBasicOperator;
import org.apache.heron.streamlet.IStreamletOperator;
import org.apache.heron.streamlet.IStreamletRichOperator;
import org.apache.heron.streamlet.IStreamletWindowOperator;
import org.apache.heron.streamlet.StreamGrouper;
import org.apache.heron.streamlet.impl.StreamletImpl;

/**
 * CustomStreamlet represents a Streamlet that is made up of applying the user
 * supplied custom operator to each element of the parent streamlet.
 */
public class CustomStreamlet<R, T> extends StreamletImpl<T> {
  private StreamletImpl<R> parent;
  private IStreamletOperator<R, T> operator;
  private Optional<StreamGrouper> grouper;

  /**
   * Create a custom streamlet from user defined CustomOperator object.
   * @param parent The parent(upstream) streamlet object
   * @param operator The user defined CustomOperator
   */
  public CustomStreamlet(StreamletImpl<R> parent,
                         IStreamletOperator<R, T> operator) {
    this.parent = parent;
    this.operator = operator;
    this.grouper = Optional.empty();
    setNumPartitions(parent.getNumPartitions());
  }

  /**
   * Create a custom streamlet from user defined CustomOperator object.
   * @param parent The parent(upstream) streamlet object
   * @param operator The user defined CustomeOperator
   * @param grouper The StreamGrouper to be used with the operator
   */
  public CustomStreamlet(StreamletImpl<R> parent,
                         IStreamletOperator<R, T> operator,
                         StreamGrouper grouper) {
    this.parent = parent;
    this.operator = operator;
    this.grouper = Optional.of(grouper);
    setNumPartitions(parent.getNumPartitions());
  }

  /**
   * Connect this streamlet to TopologyBuilder.
   * @param bldr The TopologyBuilder for the topology
   * @param stageNames The existing stage names
   * @return True if successful
   */
  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    // Create and set bolt
    BoltDeclarer declarer;
    if (operator instanceof IStreamletBasicOperator) {
      setDefaultNameIfNone(StreamletNamePrefix.CUSTOM, stageNames);
      IStreamletBasicOperator<R, T> op = (IStreamletBasicOperator<R, T>) operator;
      declarer = bldr.setBolt(getName(), op,  getNumPartitions());
    } else if (operator instanceof IStreamletRichOperator) {
      setDefaultNameIfNone(StreamletNamePrefix.CUSTOM_BASIC, stageNames);
      IStreamletRichOperator<R, T> op = (IStreamletRichOperator<R, T>) operator;
      declarer = bldr.setBolt(getName(), op,  getNumPartitions());
    } else if (operator instanceof IStreamletWindowOperator) {
      setDefaultNameIfNone(StreamletNamePrefix.CUSTOM_WINDOW, stageNames);
      IStreamletWindowOperator<R, T> op = (IStreamletWindowOperator<R, T>) operator;
      declarer = bldr.setBolt(getName(), op,  getNumPartitions());
    } else {
      throw new RuntimeException("Unhandled operator class is found!");
    }

    // Apply grouping
    if (grouper.isPresent()) {
      Map<String, StreamGrouping> groupingMap = grouper.get().getGroupings();
      groupingMap.forEach((id, grouping) -> declarer.grouping(parent.getName(), id, grouping));
    } else {
      declarer.noneGrouping(parent.getName());
    }

    return true;
  }
}
