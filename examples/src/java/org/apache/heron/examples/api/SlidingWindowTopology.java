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


package org.apache.heron.examples.api;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.examples.api.bolt.PrinterBolt;
import org.apache.heron.examples.api.bolt.SlidingWindowSumBolt;
import org.apache.heron.examples.api.spout.RandomIntegerSpout;

/**
 * A sample topology that demonstrates the usage of {@link org.apache.heron.api.bolt.IWindowedBolt}
 * to calculate sliding window sum.
 */
public final class SlidingWindowTopology {

  private static final Logger LOG = Logger.getLogger(SlidingWindowTopology.class.getName());

  private SlidingWindowTopology() {
  }

  /*
   * Computes tumbling window average
   */
  private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 8234864761939263530L;
    private OutputCollector collector;

    @Override
    @SuppressWarnings("HiddenField")
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
        collector) {
      this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      int sum = 0;
      List<Tuple> tuplesInWindow = inputWindow.get();
      LOG.fine("Events in current window: " + tuplesInWindow.size());
      if (tuplesInWindow.size() > 0) {
                /*
                * Since this is a tumbling window calculation,
                * we use all the tuples in the window to compute the avg.
                */
        for (Tuple tuple : tuplesInWindow) {
          sum += (int) tuple.getValue(0);
        }
        collector.emit(new Values(sum / tuplesInWindow.size()));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("avg"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("integer", new RandomIntegerSpout(), 1);
    builder.setBolt("slidingsum", new SlidingWindowSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(30), BaseWindowedBolt.Count.of(10)), 1)
        .shuffleGrouping("integer");
    builder.setBolt("tumblingavg", new TumblingWindowAvgBolt()
        .withTumblingWindow(BaseWindowedBolt.Count.of(3)), 1)
        .shuffleGrouping("slidingsum");
    builder.setBolt("printer", new PrinterBolt(), 1)
        .shuffleGrouping("tumblingavg");
    Config conf = new Config();
    conf.setDebug(true);
    String topoName = "test";

    conf.setComponentRam("integer", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("slidingsum", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("tumblingavg", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("printer", ByteAmount.fromGigabytes(1));

    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(5));
    conf.setContainerCpuRequested(4);

    if (args != null && args.length > 0) {
      topoName = args[0];
    }
    HeronSubmitter.submitTopology(topoName, conf, builder.createTopology());
  }
}
