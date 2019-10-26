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

package org.apache.heron.examples.api.bolt;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;

/**
 * Computes sliding window sum
 */
public class SlidingWindowSumBolt extends BaseWindowedBolt {
  private static final Logger LOG = Logger.getLogger(SlidingWindowSumBolt.class.getName());
  private static final long serialVersionUID = 1490605223057444309L;

  private int sum = 0;
  private OutputCollector collector;

  @Override
  @SuppressWarnings("HiddenField")
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
      collector) {
    this.collector = collector;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
            /*
             * The inputWindow gives a view of
             * (a) all the events in the window
             * (b) events that expired since last activation of the window
             * (c) events that newly arrived since last activation of the window
             */
    List<Tuple> tuplesInWindow = inputWindow.get();
    List<Tuple> newTuples = inputWindow.getNew();
    List<Tuple> expiredTuples = inputWindow.getExpired();

    LOG.fine("Events in current window: " + tuplesInWindow.size());
            /*
             * Instead of iterating over all the tuples in the window to compute
             * the sum, the values for the new events are added and old events are
             * subtracted. Similar optimizations might be possible in other
             * windowing computations.
             */
    for (Tuple tuple : newTuples) {
      sum += (int) tuple.getValue(0);
    }
    for (Tuple tuple : expiredTuples) {
      sum -= (int) tuple.getValue(0);
    }
    collector.emit(new Values(sum));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sum"));
  }
}
