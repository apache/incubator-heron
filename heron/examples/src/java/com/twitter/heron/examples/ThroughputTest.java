package com.twitter.heron.examples;

import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ThroughputTest {
  public static class GenSpout extends BaseRichSpout {
    private static final Character[] CHARS = new Character[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};

    SpoutOutputCollector _collector;
    int _size;
    Random _rand;
    String _id;
    String _val;

    public GenSpout() {
      _size = 8;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
      _id = randString(5);
      _val = randString(_size);
    }

    @Override
    public void nextTuple() {
      _collector.emit(new Values(_id, _val));

    }

    private String randString(int size) {
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < size; i++) {
        buf.append(CHARS[_rand.nextInt(CHARS.length)]);
      }
      return buf.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "item"));
    }
  }
    
    /*
    public static class IdentityBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "item"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(tuple.getValues());
        }        
    }

    public static class CountBolt extends BaseBasicBolt {
        int _count;
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("count"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            _count+=1;
            collector.emit(new Values(_count));
        }        
    }
    */

  public static class AckBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    }
  }


  //storm jar storm-benchmark-0.0.1-SNAPSHOT-standalone.jar storm.benchmark.ThroughputTest demo 100 8 8 8 10000
  public static void main(String[] args) throws Exception {
    int size = Integer.parseInt(args[1]);
    assert size == 8;
    int workers = Integer.parseInt(args[2]);
    int spout = Integer.parseInt(args[3]);
    int bolt = Integer.parseInt(args[4]);
    int maxPending = Integer.parseInt(args[5]);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new GenSpout(), spout);
//        builder.setBolt("count", new CountBolt(), bolt)
//                .fieldsGrouping("bolt", new Fields("id"));
//        builder.setBolt("bolt", new IdentityBolt(), bolt)
//                .shuffleGrouping("spout");
    builder.setBolt("bolt2", new AckBolt(), bolt)
        .shuffleGrouping("spout");
//        builder.setBolt("count2", new CountBolt(), bolt)
//                .fieldsGrouping("bolt2", new Fields("id"));

    Config conf = new Config();
    conf.setNumStmgrs(workers);
    //conf.setMaxSpoutPending(maxPending);
    conf.setEnableAcking(false);
//        conf.setStatsSampleRate(0.0001);
    //topology.executor.receive.buffer.size: 8192 #batched
    //topology.executor.send.buffer.size: 8192 #individual messages
    //topology.transfer.buffer.size: 1024 # batched

    //conf.put("topology.executor.send.buffer.size", 1024);
    //conf.put("topology.transfer.buffer.size", 8);
    //conf.put("topology.receiver.buffer.size", 8);
    //conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xdebug -Xrunjdwp:transport=dt_socket,address=1%ID%,server=y,suspend=n");

    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
