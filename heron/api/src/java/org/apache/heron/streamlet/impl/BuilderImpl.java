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

package org.apache.heron.streamlet.impl;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.SerializableSupplier;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.Streamlet;

/**
 * BuilderImpl implements the Builder interface.
 * The builder keeps track of the all the sources of the
 * computation dag and builds the Topology by traversing all
 * the computation nodes.
 */
public final class BuilderImpl implements Builder {
  private List<StreamletImpl<?>> sources;
  public BuilderImpl() {
    sources = new LinkedList<>();
  }

  @Override
  public <R> Streamlet<R> newSource(SerializableSupplier<R> supplier) {
    StreamletImpl<R> retval = StreamletImpl.createSupplierStreamlet(supplier);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  @Override
  public <R> Streamlet<R> newSource(Source<R> generator) {
    StreamletImpl<R> retval = StreamletImpl.createGeneratorStreamlet(generator);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  /**
   * We start traversing from all sources and build each node.
   * @return TopologyBuilder class that represents the built topology
   */
  public TopologyBuilder build() {
    TopologyBuilder builder = new TopologyBuilder();
    return build(builder);
  }

  public TopologyBuilder build(TopologyBuilder builder) {
    Set<String> stageNames = new HashSet<>();
    for (StreamletImpl<?> streamlet : sources) {
      streamlet.build(builder, stageNames);
    }
    for (StreamletImpl<?> streamlet : sources) {
      if (!streamlet.allBuilt()) {
        throw new RuntimeException("Topology cannot be fully built! Are all sources added?");
      }
    }
    return builder;
  }
}
