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

import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.dsl.SerializableFunction;

/**
 * FlatMapOperator is the class that implements the flatMap functionality.
 * It takes in the flatMapFunction Function as the input.
 * For every tuple, it applies the flatMapFunction, flattens the resulting
 * tuples and emits them
 */
public class FlatMapOperator<R, T> extends DslOperator {
  private static final long serialVersionUID = -2418329215159618998L;
  private SerializableFunction<? super R, ? extends Iterable<? extends T>> flatMapFn;

  private OutputCollector collector;

  public FlatMapOperator(
      SerializableFunction<? super R, ? extends Iterable<? extends T>> flatMapFn) {
    this.flatMapFn = flatMapFn;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    Iterable<? extends T> result = flatMapFn.apply(obj);
    for (T o : result) {
      collector.emit(new Values(o));
    }
    collector.ack(tuple);
  }
}
