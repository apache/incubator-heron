//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.dsl.impl.sinks;

import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.dsl.Context;
import com.twitter.heron.dsl.Sink;
import com.twitter.heron.dsl.impl.ContextImpl;
import com.twitter.heron.dsl.impl.operators.DslOperator;

/**
 * ConsumerSink is a very simple Sink that basically invokes a user supplied
 * consume function for every tuple.
 */
public class ComplexSink<R> extends DslOperator {
  private static final Logger LOG = Logger.getLogger(ComplexSink.class.getName());
  private static final long serialVersionUID = 8717991188885786658L;
  private Sink<R> sink;
  private OutputCollector collector;
  private Context context;
  private State<Serializable, Serializable> state;

  public ComplexSink(Sink<R> sink) {
    this.sink = sink;
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    this.collector = outputCollector;
    context = new ContextImpl(topologyContext, map, state);
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
