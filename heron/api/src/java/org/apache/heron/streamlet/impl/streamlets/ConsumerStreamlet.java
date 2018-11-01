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
import org.apache.heron.streamlet.SerializableConsumer;
import org.apache.heron.streamlet.impl.StreamletImpl;
import org.apache.heron.streamlet.impl.sinks.ConsumerSink;

/**
 * ConsumerStreamlet represents an empty Streamlet that is made up of elements from the parent
 * streamlet after consuming every element. Since elements of the parents are just consumed
 * by the user passed consumer function, nothing is emitted, thus this streamlet is empty.
 */
public class ConsumerStreamlet<R> extends StreamletImpl<R> {
  private StreamletImpl<R> parent;
  private SerializableConsumer<R> consumer;

  public ConsumerStreamlet(StreamletImpl<R> parent, SerializableConsumer<R> consumer) {
    this.parent = parent;
    this.consumer = consumer;
    setNumPartitions(parent.getNumPartitions());
  }

  /**
   * Create a custom streamlet from user defined CustomWindowOperator object.
   * @param parent The parent(upstream) streamlet object
   * @param operator The user defined CustomeWindowOperator
   * @param grouper The StreamGrouper to be used with the operator
   */
  public CustomWindowStreamlet(StreamletImpl<R> parent,
                               IStreamletWindowOperator<R, T> operator,
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
    setDefaultNameIfNone(StreamletNamePrefix.CONSUMER, stageNames);
    bldr.setBolt(getName(), new ConsumerSink<>(consumer),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
