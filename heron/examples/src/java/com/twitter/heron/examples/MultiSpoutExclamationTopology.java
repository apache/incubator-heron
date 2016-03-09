package com.twitter.heron.examples;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

// TODO:- implement this
// import backtype.storm.LocalCluster;

/**
 * This is a basic example of a Storm topology.
 */
public class MultiSpoutExclamationTopology {
  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;
    long nItems;
    long startTime;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      // System.out.println(tuple.getString(0));
      // _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      //  _collector.ack(tuple);
      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Done " + nItems + " in " + latency);
        GlobalMetrics.incr("selected_items");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      // declarer.declare(new Fields("word"));
    }
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
    //builder.setBolt("exclaim2", new ExclamationBolt(), 2)
    //        .shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentRam("word0", 500 * 1024 * 1024);
    conf.setComponentRam("word1", 500 * 1024 * 1024);
    conf.setComponentRam("word2", 500 * 1024 * 1024);
    conf.setComponentRam("exclaim1", 1024 * 1024 * 1024);

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(1);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      // TODO:- This is not yet supported
      System.out.println("Local mode not yet supported");
      System.exit(1);
      /*
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
      */
    }
  }
}
