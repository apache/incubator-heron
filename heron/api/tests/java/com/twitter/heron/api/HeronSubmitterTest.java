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
package com.twitter.heron.api;

import java.util.Map;

import org.junit.Test;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.common.basics.ByteAmount;

public class HeronSubmitterTest {

  public static class TestSpout extends BaseRichSpout {

    private static final long serialVersionUID = -630307949908406294L;

    @SuppressWarnings("rawtypes")
    public void open(
        Map conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
    }

    public void close() {
    }

    public void nextTuple() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

  public static class TestBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -5888421647633083078L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

  @Test(expected = InvalidTopologyException.class)
  public void testInvalidTopologySubmittion()
      throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = new TopologyBuilder();

    int spouts = 2;
    int bolts = 2;
    builder.setSpout("word", new TestSpout(), spouts);
    builder.setBolt("exclaim1", new TestBolt(), bolts)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentRam("word", ByteAmount.fromMegabytes(10));
    conf.setComponentRam("exclaim1", ByteAmount.fromMegabytes(10));

    // container resource configuration
    conf.setContainerDiskRequested(
        ByteAmount.fromMegabytes(10));
    conf.setContainerRamRequested(
        ByteAmount.fromMegabytes(10));
    conf.setContainerCpuRequested(1);

    // Set the number of workers or stream managers
    conf.setNumStmgrs(2);
    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }
}
