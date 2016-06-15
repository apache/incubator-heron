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
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

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

    builder.setSpout("word", new AckingTestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(), 2)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking, we need to setEnableAcking true
    conf.setEnableAcking(true);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // Set the number of workers or stream managers
    conf.setNumStmgrs(1);
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingTestWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -630307949908406294L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;

    public AckingTestWordSpout() {
    }

    public void open(
        Map<String, Object> conf,
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
