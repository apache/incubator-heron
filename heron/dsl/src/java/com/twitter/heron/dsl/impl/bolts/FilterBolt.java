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

package com.twitter.heron.dsl.impl.bolts;

import java.util.Map;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.dsl.SerializablePredicate;

/**
 * FilterBolt implements the  functionality of the filter operation
 * It takes in a filterFunction predicate as the input.
 * For every tuple that it encounters, the filter function is run
 * and the tuple is re-emitted if the predicate evaluates to true
 */
public class FilterBolt<R> extends DslBolt {
  private static final long serialVersionUID = -4748646871471052706L;
  private SerializablePredicate<? super R> filterFn;

  private OutputCollector collector;

  public FilterBolt(SerializablePredicate<? super R> filterFn) {
    this.filterFn = filterFn;
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
    if (filterFn.test(obj)) {
      collector.emit(new Values(obj));
    }
    collector.ack(tuple);
  }
}
