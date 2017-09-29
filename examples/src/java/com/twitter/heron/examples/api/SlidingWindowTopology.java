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

package com.twitter.heron.examples.api;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.PrinterBolt;
import com.twitter.heron.examples.api.bolt.SlidingWindowSumBolt;
import com.twitter.heron.examples.api.spout.RandomIntegerSpout;

/**
 * A sample topology that demonstrates the usage of {@link com.twitter.heron.api.bolt.IWindowedBolt}
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

    Config.setComponentRam(conf, "integer", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "slidingsum", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "tumblingavg", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "printer", ByteAmount.fromGigabytes(1));

    Config.setContainerDiskRequested(conf, ByteAmount.fromGigabytes(5));
    Config.setContainerCpuRequested(conf, 4);

    if (args != null && args.length > 0) {
      topoName = args[0];
    }
    HeronSubmitter.submitTopology(topoName, conf, builder.createTopology());
  }
}
