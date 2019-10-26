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
import java.util.Random;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.simulator.Simulator;

/**
 * This is three stage topology. Spout emits to bolt to bolt.
 */
public final class MultiStageAckingTopology {

  private MultiStageAckingTopology() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("Please specify the name of the topology");
    }
    TopologyBuilder builder = new TopologyBuilder();

    int parallelism = 2;
    builder.setSpout("word", new AckingTestWordSpout(), parallelism);
    builder.setBolt("exclaim1", new ExclamationBolt(true), parallelism)
        .shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(false), parallelism)
        .shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking, we need to setEnableAcking true
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentRam("word",
        ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1",
        ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim2",
        ExampleResources.getComponentRam());

    // container resource configuration
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(3 * parallelism, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(3 * parallelism, parallelism));
    conf.setContainerCpuRequested(
        ExampleResources.getContainerCpu(3 * parallelism, parallelism));

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(parallelism);
      HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      Simulator simulator = new Simulator();
      simulator.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      simulator.killTopology("test");
      simulator.shutdown();
    }
  }

  public static class AckingTestWordSpout extends BaseRichSpout {
    private static final long serialVersionUID = -5972291205871728684L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;

    public AckingTestWordSpout() {
    }

    @SuppressWarnings("rawtypes")
    public void open(
        Map conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
      collector = acollector;
      words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      rand = new Random();
    }

    public void close() {
    }

    public void nextTuple() {
      // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
      Utils.sleep(1);
      final String word = words[rand.nextInt(words.length)];
      // To enable acking, we need to emit tuple with MessageId, which is an object
      collector.emit(new Values(word), "MESSAGE_ID");
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {
    private static final long serialVersionUID = -3226618846531432832L;
    private OutputCollector collector;
    private long nItems;
    private long startTime;
    private boolean emit;

    public ExclamationBolt(boolean emit) {
      this.emit = emit;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
        Map conf,
        TopologyContext context,
        OutputCollector acollector) {
      collector = acollector;
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      // We need to ack a tuple when we consider it is done successfully
      // Or we could fail it by invoking collector.fail(tuple)
      // If we do not do the ack or fail explicitly
      // After the MessageTimeout Seconds, which could be set in Config,
      // the spout will fail this tuple
      ++nItems;
      if (nItems % 10000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
      }
      if (emit) {
        collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      }
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      if (emit) {
        declarer.declare(new Fields("word"));
      }
    }
  }
}
