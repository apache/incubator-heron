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

package com.twitter.heron.dsl.impl.operators;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.dsl.Context;
import com.twitter.heron.dsl.TransformFunction;
import com.twitter.heron.dsl.impl.ContextImpl;

/**
 * TransformOperator is the class that implements the transform functionality.
 * It takes in the transformFunction Function as the input.
 * It calls the transformFunction setup/cleanup at the beginning/end of the
 * processing. And for every tuple, it applies the transformFunction, and emits the resulting value
 */
public class TransformOperator<R, T> extends DslOperator {
  private static final long serialVersionUID = 429297144878185182L;
  private TransformFunction<? super R, ? extends T> transformFunction;

  private OutputCollector collector;
  private Context context;
  private State<Serializable, Serializable> state;

  public TransformOperator(TransformFunction<? super R, ? extends T> transformFunction) {
    this.transformFunction = transformFunction;
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @Override
  public void cleanup() {
    transformFunction.cleanup();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
    context = new ContextImpl(topologyContext, map, state);
    transformFunction.setup(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    transformFunction.transform(obj, x -> collector.emit(new Values(x)));
    collector.ack(tuple);
  }
}
