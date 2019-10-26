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

package org.apache.heron.simulator.utils;

import java.io.Serializable;
import java.time.Duration;
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

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.simulator.grouping.Grouping;


/**
 * TopologyManager Tester.
 */
public class TopologyManagerTest implements Serializable {
  private static final long serialVersionUID = -8532695494712446731L;
  public static final String TOPOLOGY_NAME = "topology-name";
  public static final String TOPOLOGY_ID = "topology-id";
  public static final String BOLT_ID = "exclaim";
  public static final String STREAM_ID = "word";

  private static TopologyManager topologyManager;
  private static TopologyAPI.Topology topology;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = TopologyManagerTest.getTestTopology();
    topologyManager = new TopologyManager(topology);
  }

  @AfterClass
  public static void afterClass() throws Exception {

  }

  /**
   * Construct the test topology
   */
  public static TopologyAPI.Topology getTestTopology() {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout(STREAM_ID, new BaseRichSpout() {
      private static final long serialVersionUID = 5406114907377311020L;

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(STREAM_ID));
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

    topologyBuilder.setBolt(BOLT_ID, new BaseBasicBolt() {
      private static final long serialVersionUID = 4398578755681473899L;

      @Override
      public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

      }
    }, 2)
        .shuffleGrouping(STREAM_ID);

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentRam(STREAM_ID, ByteAmount.fromMegabytes(500));
    conf.setComponentRam(BOLT_ID,  ByteAmount.fromGigabytes(1));
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
   * Method: getPhysicalPlan()
   */
  @Test
  public void testGetPhysicalPlan() throws Exception {
    Assert.assertEquals(topology, topologyManager.getPhysicalPlan().getTopology());

    Assert.assertEquals(1, topologyManager.getPhysicalPlan().getStmgrsCount());
    PhysicalPlans.StMgr stMgr = topologyManager.getPhysicalPlan().getStmgrs(0);
    Assert.assertEquals("", stMgr.getId());
    Assert.assertEquals("", stMgr.getHostName());
    Assert.assertEquals(-1, stMgr.getDataPort());
    Assert.assertEquals("", stMgr.getLocalEndpoint());
    Assert.assertEquals("", stMgr.getCwd());

    Assert.assertEquals(4, topologyManager.getPhysicalPlan().getInstancesCount());

    ArrayList<String> componentsName = new ArrayList<>();
    ArrayList<String> instancesId = new ArrayList<>();
    ArrayList<Integer> componentsIndex = new ArrayList<>();

    for (int i = 0; i < topologyManager.getPhysicalPlan().getInstancesCount(); i++) {
      PhysicalPlans.Instance instance = topologyManager.getPhysicalPlan().getInstances(i);
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
   * Method: getComponentToTaskIds()
   */
  @Test
  public void testGetComponentToTaskIds() throws Exception {
    Map<String, List<Integer>> map = topologyManager.getComponentToTaskIds();

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(2, map.get(STREAM_ID).size());
    Assert.assertEquals(2, map.get(BOLT_ID).size());

    List<Integer> taskIds = new LinkedList<>();
    taskIds.addAll(map.get(STREAM_ID));
    taskIds.addAll(map.get(BOLT_ID));
    Collections.sort(taskIds);

    Assert.assertEquals("[1, 2, 3, 4]", taskIds.toString());
  }

  /**
   * Method: extractTopologyTimeout()
   */
  @Test
  public void testExtractTopologyTimeout() throws Exception {
    Assert.assertEquals(Duration.ofSeconds(1), topologyManager.extractTopologyTimeout());
  }

  /**
   * Method: getStreamConsumers()
   */
  @Test
  public void testPopulateStreamConsumers() throws Exception {
    Map<TopologyAPI.StreamId, List<Grouping>> map = topologyManager.getStreamConsumers();
    Assert.assertEquals(1, map.size());

    for (Map.Entry<TopologyAPI.StreamId, List<Grouping>> entry : map.entrySet()) {
      TopologyAPI.StreamId streamId = entry.getKey();
      Assert.assertEquals("default", streamId.getId());
      Assert.assertEquals(STREAM_ID, streamId.getComponentName());

      List<Grouping> consumers = entry.getValue();
      Assert.assertNotNull(consumers);
      Assert.assertEquals(1, consumers.size());

      Grouping grouping = consumers.get(0);
      Assert.assertTrue(grouping.getClass().toString().contains("ShuffleGrouping"));

      Assert.assertEquals(1, topologyManager.getListToSend(streamId, null).size());

      List<Integer> boltTasksId = topologyManager.getComponentToTaskIds().get(BOLT_ID);
      Integer targetId = topologyManager.getListToSend(streamId, null).get(0);
      Assert.assertTrue(boltTasksId.contains(targetId));
    }
  }
}
