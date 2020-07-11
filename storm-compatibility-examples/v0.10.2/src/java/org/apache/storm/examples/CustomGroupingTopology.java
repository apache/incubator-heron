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

package org.apache.storm.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.examples.spout.TestWordSpout;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

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

    conf.setNumWorkers(2);

    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
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
    public void prepare(
        WorkerTopologyContext context,
        GlobalStreamId stream,
        List<Integer> targetTasks) {
      this.taskIds = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
      List<Integer> ret = new ArrayList<>();
      ret.add(taskIds.get(0));
      return ret;
    }
  }
}
