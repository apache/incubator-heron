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

package org.apache.heron.streamlet.impl.sinks;

import java.io.Serializable;
import java.util.Map;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.impl.ContextImpl;
import org.apache.heron.streamlet.impl.operators.StreamletOperator;

/**
 * ConsumerSink is a very simple Sink that basically invokes a user supplied
 * consume function for every tuple.
 */
public class ComplexSink<R> extends StreamletOperator<R, R>
    implements IStatefulComponent<Serializable, Serializable> {

  private static final long serialVersionUID = 8717991188885786658L;
  private Sink<R> sink;
  private State<Serializable, Serializable> state;

  public ComplexSink(Sink<R> sink) {
    this.sink = sink;
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @Override
  public void preSave(String checkpointId) {
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    this.collector = outputCollector;
    Context context = new ContextImpl(topologyContext, map, state);
    sink.setup(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    sink.put(obj);
    collector.ack(tuple);
  }
}
