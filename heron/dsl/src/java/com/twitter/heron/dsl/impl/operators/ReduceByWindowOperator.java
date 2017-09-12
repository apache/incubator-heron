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
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.SerializableBinaryOperator;
import com.twitter.heron.dsl.WindowInfo;

/**
 * ReduceByWindowOperator is the class that implements the reduceByWindow functionality.
 * It takes in a reduceFn Function as input.
 * For every window, the bolt applies reduceFn to all the tuples in that window, and emits
 * the resulting value as output
 */
public class ReduceByWindowOperator<I> extends DslWindowOperator {
  private static final long serialVersionUID = 6513775685209414130L;
  private SerializableBinaryOperator<I> reduceFn;
  private OutputCollector collector;

  public ReduceByWindowOperator(SerializableBinaryOperator<I> reduceFn) {
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(TupleWindow inputWindow) {
    I reducedValue = null;
    for (Tuple tuple : inputWindow.get()) {
      I tup = (I) tuple.getValue(0);
      if (reducedValue == null) {
        reducedValue = tup;
      } else {
        reducedValue = reduceFn.apply(reducedValue, tup);
      }
    }
    long startWindow;
    long endWindow;
    if (inputWindow.getStartTimestamp() == null) {
      startWindow = 0;
    } else {
      startWindow = inputWindow.getStartTimestamp();
    }
    if (inputWindow.getEndTimestamp() == null) {
      endWindow = 0;
    } else {
      endWindow = inputWindow.getEndTimestamp();
    }
    WindowInfo windowInfo = new WindowInfo(startWindow, endWindow);
    collector.emit(new Values(new KeyValue<>(windowInfo, reducedValue)));
  }
}
