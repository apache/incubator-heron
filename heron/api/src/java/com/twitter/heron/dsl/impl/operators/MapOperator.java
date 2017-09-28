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
 * MapOperator is the class that implements the map functionality.
 * It takes in the mapFunction Function as the input.
 * For every tuple, it applies the mapFunction, and emits the resulting value
 */
public class MapOperator<R, T> extends DslOperator {
  private static final long serialVersionUID = -1303096133107278700L;
  private SerializableFunction<? super R, ? extends T> mapFn;

  private OutputCollector collector;

  public MapOperator(SerializableFunction<? super R, ? extends T> mapFn) {
    this.mapFn = mapFn;
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
    T result = mapFn.apply(obj);
    collector.emit(new Values(result));
    collector.ack(tuple);
  }
}
