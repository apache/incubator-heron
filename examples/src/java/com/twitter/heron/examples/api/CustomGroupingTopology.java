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
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.spout.TestWordSpout;
/**
 * This is a basic example of a Storm topology.
 */
public final class CustomGroupingTopology {

  private CustomGroupingTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 2);
    builder.setBolt("mybolt", new MyBolt(), 2)
        .customGrouping("word", new MyCustomStreamGrouping());

    Config conf = new Config();

    // component resource configuration
    com.twitter.heron.api.Config
        .setComponentRam(conf, "word", ByteAmount.fromMegabytes(512));
    com.twitter.heron.api
        .Config.setComponentRam(conf, "mybolt", ByteAmount.fromMegabytes(512));

    // container resource configuration
    com.twitter.heron.api.Config.setContainerDiskRequested(conf, ByteAmount.fromGigabytes(2));
    com.twitter.heron.api.Config.setContainerRamRequested(conf, ByteAmount.fromGigabytes(2));
    com.twitter.heron.api.Config.setContainerCpuRequested(conf, 2);

    conf.setNumStmgrs(2);

    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class MyBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1913733461146490337L;
    private long nItems;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
        Map conf,
        TopologyContext context,
        OutputCollector acollector) {
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
    private static final long serialVersionUID = 5987557161936201860L;
    private List<Integer> taskIds;

    public MyCustomStreamGrouping() {
    }

    @Override
    public void prepare(TopologyContext context,
                        String component, String streamId,
                        List<Integer> targetTasks) {
      this.taskIds = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
      List<Integer> ret = new ArrayList<>();
      ret.add(taskIds.get(0));
      return ret;
    }
  }
}
