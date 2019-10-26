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
package org.apache.heron.streamlet.impl.sources;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.ContextImpl;

/**
 * SupplierSource is a way to wrap a supplier function inside a Heron Spout.
 * The SupplierSource just calls the get method of the supplied function
 * to generate the next tuple.
 */
public class ComplexSource<R> extends StreamletSource {

  private static final long serialVersionUID = -5086763670301450007L;
  private Source<R> generator;
  private State<Serializable, Serializable> state;

  public ComplexSource(Source<R> generator) {
    this.generator = generator;
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map<String, Object> map, TopologyContext topologyContext,
                   SpoutOutputCollector outputCollector) {
    super.open(map, topologyContext, outputCollector);
    Context context = new ContextImpl(topologyContext, map, state);
    generator.setup(context);
  }

  @Override
  public void nextTuple() {
    Collection<R> tuples = generator.get();
    if (tuples != null) {
      tuples.forEach(tuple -> collector.emit(new Values(tuple)));
    }
  }
}
