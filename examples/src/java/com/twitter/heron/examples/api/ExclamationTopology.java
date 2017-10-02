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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.IUpdatable;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.api.spout.TestWordSpout;
import com.twitter.heron.simulator.Simulator;

/**
 * This is a basic example of a Storm topology.
 */
public final class ExclamationTopology {

  private ExclamationTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    int parallelism = 2;

    int spouts = parallelism;
    builder.setSpout("word", new TestWordSpout(Duration.ofMillis(0)), spouts);
    int bolts = 2 * parallelism;
    builder.setBolt("exclaim1", new ExclamationBolt(), bolts)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.setMessageTimeoutSecs(600);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // resources configuration
    com.twitter.heron.api.Config.setComponentRam(conf, "word", ExampleResources.getComponentRam());
    com.twitter.heron.api.Config.setComponentRam(conf, "exclaim1",
        ExampleResources.getComponentRam());

    com.twitter.heron.api.Config.setContainerDiskRequested(conf,
        ExampleResources.getContainerDisk(spouts + bolts, parallelism));
    com.twitter.heron.api.Config.setContainerRamRequested(conf,
        ExampleResources.getContainerRam(spouts + bolts, parallelism));
    com.twitter.heron.api.Config.setContainerCpuRequested(conf, 1);

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(parallelism);
      HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      System.out.println("Topology name not provided as an argument, running in simulator mode.");
      Simulator simulator = new Simulator();
      simulator.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      simulator.killTopology("test");
      simulator.shutdown();
    }


  }

  public static class ExclamationBolt extends BaseRichBolt implements IUpdatable {

    private static final long serialVersionUID = 1184860508880121352L;
    private long nItems;
    private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
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

    /**
     * Implementing this method is optional and only necessary if BOTH of the following are true:
     *
     * a.) you plan to dynamically scale your bolt/spout at runtime using 'heron update'.
     * b.) you need to take action based on a runtime change to the component parallelism.
     *
     * Most bolts and spouts should be written to be unaffected by changes in their parallelism,
     * but some must be aware of it. An example would be a spout that consumes a subset of queue
     * partitions, which must be algorithmically divided amongst the total number of spouts.
     * <P>
     * Note that this method is from the IUpdatable Heron interface which does not exist in Storm.
     * It is fine to implement IUpdatable along with other Storm interfaces, but implementing it
     * will bind an otherwise generic Storm implementation to Heron.
     *
     * @param heronTopologyContext Heron topology context.
     */
    @Override
    public void update(com.twitter.heron.api.topology.TopologyContext heronTopologyContext) {
      List<Integer> newTaskIds =
          heronTopologyContext.getComponentTasks(heronTopologyContext.getThisComponentId());
      System.out.println("Bolt updated with new topologyContext. New taskIds: " + newTaskIds);
    }
  }
}
