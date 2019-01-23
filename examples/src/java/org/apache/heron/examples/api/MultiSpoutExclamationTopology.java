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

import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.examples.api.spout.TestWordSpout;
import org.apache.heron.simulator.Simulator;


/**
 * This is a basic example of a Storm topology.
 */
public final class MultiSpoutExclamationTopology {

  private MultiSpoutExclamationTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word0", new TestWordSpout(), 2);
    builder.setSpout("word1", new TestWordSpout(), 2);
    builder.setSpout("word2", new TestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(), 2)
        .shuffleGrouping("word0")
        .shuffleGrouping("word1")
        .shuffleGrouping("word2");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentRam("word0", ExampleResources.getComponentRam());
    conf.setComponentRam("word1", ExampleResources.getComponentRam());
    conf.setComponentRam("word2", ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1", ExampleResources.getComponentRam());

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(5));
    conf.setContainerCpuRequested(4);

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(3);
      HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      Simulator simulator = new Simulator();
      simulator.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      simulator.killTopology("test");
      simulator.shutdown();
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6945654705222426596L;
    private long nItems;
    private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf,
                        TopologyContext context, OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      // System.out.println(tuple.getString(0));
      // collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      // collector.ack(tuple);
      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      // declarer.declare(new Fields("word"));
    }
  }
}
