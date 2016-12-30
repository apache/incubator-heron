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

package com.twitter.heron.simulator.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.proto.system.PhysicalPlans;


/**
 * PhysicalPlanGenerator Tester.
 */
public class PhysicalPlanUtilTest implements Serializable {
  private static final long serialVersionUID = -8532695494712446731L;
  public static final String TOPOLOGY_NAME = "topology-name";
  public static final String TOPOLOGY_ID = "topology-id";

  private static PhysicalPlans.PhysicalPlan plan;
  private static TopologyAPI.Topology topology;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = PhysicalPlanUtilTest.getTestTopology();
    plan = PhysicalPlanUtil.getPhysicalPlan(topology);
  }

  @AfterClass
  public static void afterClass() throws Exception {

  }

  /**
   * Construct the test topology
   */
  public static TopologyAPI.Topology getTestTopology() {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout("word", new BaseRichSpout() {
      private static final long serialVersionUID = 5406114907377311020L;

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
      }

      @Override
      public void open(
          Map<String, Object> map,
          TopologyContext topologyContext,
          SpoutOutputCollector spoutOutputCollector) {
      }

      @Override
      public void nextTuple() {

      }
    }, 2);

    topologyBuilder.setBolt("exclaim", new BaseBasicBolt() {
      private static final long serialVersionUID = 4398578755681473899L;

      @Override
      public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

      }
    }, 2)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentRam("word", ByteAmount.fromMegabytes(500));
    conf.setComponentRam("exclaim",  ByteAmount.fromGigabytes(1));
    conf.setMessageTimeoutSecs(1);

    return topologyBuilder.createTopology().
        setName("topology-name").
        setConfig(conf).
        setState(TopologyAPI.TopologyState.RUNNING).
        getTopology();
  }

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: getPhysicalPlan(TopologyAPI.Topology topology)
   */
  @Test
  public void testGetPhysicalPlan() throws Exception {
    Assert.assertEquals(topology, plan.getTopology());

    Assert.assertEquals(1, plan.getStmgrsCount());
    PhysicalPlans.StMgr stMgr = plan.getStmgrs(0);
    Assert.assertEquals("", stMgr.getId());
    Assert.assertEquals("", stMgr.getHostName());
    Assert.assertEquals(-1, stMgr.getDataPort());
    Assert.assertEquals("", stMgr.getLocalEndpoint());
    Assert.assertEquals("", stMgr.getCwd());

    Assert.assertEquals(4, plan.getInstancesCount());

    ArrayList<String> componentsName = new ArrayList<>();
    ArrayList<String> instancesId = new ArrayList<>();
    ArrayList<Integer> componentsIndex = new ArrayList<>();

    for (int i = 0; i < plan.getInstancesCount(); i++) {
      PhysicalPlans.Instance instance = plan.getInstances(i);
      Assert.assertEquals("", instance.getStmgrId());
      Assert.assertEquals(i + 1, instance.getInfo().getTaskId());

      componentsName.add(instance.getInfo().getComponentName());
      instancesId.add(instance.getInstanceId());
      componentsIndex.add(instance.getInfo().getComponentIndex());
    }

    // Sort to guarantee the order to make the unit test deterministic
    Collections.sort(componentsName);
    Collections.sort(instancesId);
    Collections.sort(componentsIndex);

    Assert.assertEquals("[exclaim, exclaim, word, word]", componentsName.toString());
    Assert.assertEquals("[exclaim_1, exclaim_2, word_1, word_2]", instancesId.toString());
    Assert.assertEquals("[1, 1, 2, 2]", componentsIndex.toString());
  }

  /**
   * Method: getComponentToTaskIds(PhysicalPlans.PhysicalPlan physicalPlan)
   */
  @Test
  public void testGetComponentToTaskIds() throws Exception {
    Map<String, List<Integer>> map = PhysicalPlanUtil.getComponentToTaskIds(plan);

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(2, map.get("word").size());
    Assert.assertEquals(2, map.get("exclaim").size());

    List<Integer> taskIds = new LinkedList<>();
    taskIds.addAll(map.get("word"));
    taskIds.addAll(map.get("exclaim"));
    Collections.sort(taskIds);

    Assert.assertEquals("[1, 2, 3, 4]", taskIds.toString());
  }

  /**
   * Method: extractTopologyTimeout(TopologyAPI.Topology topology)
   */
  @Test
  public void testExtractTopologyTimeout() throws Exception {
    Assert.assertEquals(1, PhysicalPlanUtil.extractTopologyTimeout(topology));
  }
}
