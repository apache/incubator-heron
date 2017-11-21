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

import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseStatefulWindowedBolt;
import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.PrinterBolt;

/**
 * A sample topology that demonstrates the usage of {@link com.twitter.heron.api.bolt.IStatefulWindowedBolt}
 * to calculate sliding window sum.  Topology also demonstrates how stateful window processing is done
 * in conjunction with effectively once guarantees
 */
public final class StatefulSlidingWindowTopology {

  private static final Logger LOG = Logger.getLogger(StatefulSlidingWindowTopology.class.getName());

  private StatefulSlidingWindowTopology() {
  }

  private static class WindowSumBolt extends BaseStatefulWindowedBolt<String, Long> {
    private static final long serialVersionUID = -539382497249834244L;
    private State<String, Long> state;
    private long sum;

    private OutputCollector collector;

    @Override
    @SuppressWarnings("HiddenField")
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void initState(State<String, Long> state) {
      this.state = state;
      sum = state.getOrDefault("sum", 0L);
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      for (Tuple tuple : inputWindow.get()) {
        sum += tuple.getLongByField("value");
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

  public static class IntegerSpout extends BaseRichSpout
      implements IStatefulComponent<String, Long> {
    private static final Logger LOG = Logger.getLogger(IntegerSpout.class.getName());
    private static final long serialVersionUID = 5454291010750852782L;
    private SpoutOutputCollector collector;
    private Random rand;
    private long msgId;
    private State<String, Long> state;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("value", "ts", "msgid"));
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector
        collector) {
      this.collector = collector;
      this.rand = new Random();
    }

    @Override
    public void nextTuple() {
      Utils.sleep(1000);
      long val = msgId;
      collector.emit(new Values(val,
          System.currentTimeMillis() - (24 * 60 * 60 * 1000), msgId), msgId);
      msgId++;
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void ack(Object msgId) {
      LOG.fine("Got ACK for msgId : " + msgId);
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void fail(Object msgId) {
      LOG.fine("Got FAIL for msgId : " + msgId);
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void initState(State<String, Long> state) {
      this.state = state;
      this.msgId = this.state.getOrDefault("msgId", 0L);
    }

    @Override
    public void preSave(String checkpointId) {
      this.state.put("msgId", msgId);
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("integer", new IntegerSpout(), 1);
    builder.setBolt("sumbolt", new WindowSumBolt().withWindow(BaseWindowedBolt.Count.of(5),
        BaseWindowedBolt.Count.of(3)), 1).shuffleGrouping("integer");
    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("sumbolt");
    Config conf = new Config();
    conf.setDebug(true);
    String topoName = "test";

    Config.setComponentRam(conf, "integer", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "sumbolt", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "printer", ByteAmount.fromGigabytes(1));

    Config.setContainerDiskRequested(conf, ByteAmount.fromGigabytes(5));
    Config.setContainerCpuRequested(conf, 4);

    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);
    conf.setMaxSpoutPending(1000);

    if (args != null && args.length > 0) {
      topoName = args[0];
    }
    HeronSubmitter.submitTopology(topoName, conf, builder.createTopology());
  }
}
