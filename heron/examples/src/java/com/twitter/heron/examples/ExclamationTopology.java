// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.examples;

import java.util.Map;

import com.twitter.heron.common.basics.ByteAmount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * This is a basic example of a Storm topology.
 */
public final class ExclamationTopology {

  private ExclamationTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    int parallelism = 2;

    builder.setSpout("word", new TestWordSpout(), parallelism);
    builder.setBolt("exclaim1", new ExclamationBolt(), 2 * parallelism)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentRam("word", ByteAmount.fromGigabytes(3));
    conf.setComponentRam("exclaim1", ByteAmount.fromGigabytes(3));
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(5));
    conf.setContainerCpuRequested(5);

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(parallelism);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      System.out.println("Topology name not provided as an argument, running in simulator mode.");
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1184860508880121352L;
    private long nItems;
    private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
        Map conf,
        TopologyContext context,
        OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println(tuple.getString(0) + "!!!");
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
