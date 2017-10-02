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

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;


/**
 * This is a basic example of a Heron topology with acking enable.
 */
public final class AckingTopology {

  private AckingTopology() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("Specify topology name");
    }
    TopologyBuilder builder = new TopologyBuilder();

    int spouts = 2;
    int bolts = 2;
    builder.setSpout("word", new AckingTestWordSpout(), spouts);
    builder.setBolt("exclaim1", new ExclamationBolt(), bolts)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    com.twitter.heron.api.Config.setComponentRam(conf, "word", ExampleResources.getComponentRam());
    com.twitter.heron.api.Config.setComponentRam(conf, "exclaim1",
        ExampleResources.getComponentRam());

    // container resource configuration
    com.twitter.heron.api.Config.setContainerDiskRequested(conf,
        ExampleResources.getContainerDisk(spouts + bolts, 2));
    com.twitter.heron.api.Config.setContainerRamRequested(conf,
        ExampleResources.getContainerRam(spouts + bolts, 2));
    com.twitter.heron.api.Config.setContainerCpuRequested(conf, 1);

    // Set the number of workers or stream managers
    conf.setNumStmgrs(2);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingTestWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -630307949908406294L;
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
    private static final long serialVersionUID = -2267338658317778214L;
    private OutputCollector collector;
    private long nItems;
    private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector acollector) {
      collector = acollector;
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      // We need to ack a tuple when we consider it is done successfully
      // Or we could fail it by invoking collector.fail(tuple)
      // If we do not do the ack or fail explicitly
      // After the MessageTimeout Seconds, which could be set in Config, the spout will
      // fail this tuple
      ++nItems;
      if (nItems % 10000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
        // Here we explicitly forget to do the ack or fail
        // It would trigger fail on this tuple on spout end after MessageTimeout Seconds
      } else if (nItems % 2 == 0) {
        collector.fail(tuple);
      } else {
        collector.ack(tuple);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }
}
