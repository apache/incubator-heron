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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.api.GlobalMetrics;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

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

    builder.setSpout("word", new AckingTestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(true), 2)
        .shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(false), 2)
        .shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking, we need to setEnableAcking true
    conf.setEnableAcking(true);

    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setNumStmgrs(1);
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
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
