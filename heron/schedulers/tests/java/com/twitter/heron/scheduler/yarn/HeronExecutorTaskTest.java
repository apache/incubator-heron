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

package com.twitter.heron.scheduler.yarn;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.api.generated.TopologyAPI.TopologyState;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

public class HeronExecutorTaskTest {
  @Test
  public void providesConfigsNeededForExecutorCmd() throws Exception {
    Topology testTopology = createTestTopology("testTopology");

    HeronExecutorTask task = new HeronExecutorTask(null,
        "5",
        "cluster",
        "role",
        "testTopology",
        "env",
        "package",
        "core",
        "jar",
        "packedPlan",
        "componentRamMap",
        false);
    HeronExecutorTask spyTask = Mockito.spy(task);

    Mockito.doReturn("file").when(spyTask).getTopologyDefnFile();
    Mockito.doReturn(testTopology).when(spyTask).getTopology("file");
    String[] command = spyTask.getExecutorCommand();

    // only two configs; state manager root and url should be null.
    int nullCounter = 2;
    for (String subCommand : command) {
      if (subCommand == null) {
        nullCounter--;
      }
    }
    Assert.assertEquals(0, nullCounter);
  }

  Topology createTestTopology(String name) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout-1", new TestSpout(), 2);
    builder.setBolt("bolt-1", new TestBolt(), 1).shuffleGrouping("spout-1");
    HeronTopology topology = builder.createTopology();
    com.twitter.heron.api.Config config = new com.twitter.heron.api.Config();
    return topology.setName(name).setConfig(config).setState(TopologyState.RUNNING).getTopology();
  }

  public static class TestBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class TestSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;

    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }
}
