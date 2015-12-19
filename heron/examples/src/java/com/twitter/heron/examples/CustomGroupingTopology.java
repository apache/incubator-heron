package com.twitter.heron.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

/**
 * This is a basic example of a Storm topology.
 */
public class CustomGroupingTopology {
  public static class MyBolt extends BaseRichBolt {
    private long nItems;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      nItems = 0;
    }

    @Override
    public void execute(Tuple tuple) {
      if (++nItems % 10000 == 0) {
        System.out.println(tuple.getString(0));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class MyCustomStreamGrouping implements CustomStreamGrouping {
    private List<Integer> taskIds;

    public MyCustomStreamGrouping() {
    }

    @Override
    public void prepare(TopologyContext context, String component, String streamId, List<Integer> targetTasks) {
      this.taskIds = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
      List<Integer> ret = new ArrayList<Integer>();
      ret.add(taskIds.get(0));
      return ret;
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 2);
    builder.setBolt("mybolt", new MyBolt(), 2)
        .customGrouping("word", new MyCustomStreamGrouping());

    Config conf = new Config();

    conf.setNumStmgrs(1);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
