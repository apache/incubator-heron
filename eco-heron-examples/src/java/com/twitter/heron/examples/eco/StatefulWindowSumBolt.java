//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.examples.eco;

import java.util.Map;

import com.twitter.heron.api.bolt.BaseStatefulWindowedBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;

@SuppressWarnings("HiddenField")
public class StatefulWindowSumBolt extends BaseStatefulWindowedBolt<String, Long> {
  private static final long serialVersionUID = -539382497249834244L;
  private State<String, Long> state;
  private long sum;

  private OutputCollector collector;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void initState(State<String, Long> state) {
    this.state = state;
    sum = state.getOrDefault("sum", 0L);
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    for (Tuple tuple : inputWindow.get()) {
      System.out.println("Adding to sum: " + tuple.getLongByField("value"));
      sum += tuple.getLongByField("value");
      System.out.println("Sum is now: " + sum);
    }
    collector.emit(new Values(sum));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sum"));
  }

  @Override
  public void preSave(String checkpointId) {
    state.put("sum", sum);
  }
}
