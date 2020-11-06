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

package org.apache.heron.grouping;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.ExecutorTester;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.proto.system.HeronTuples;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.resource.TestBolt;
import org.apache.heron.resource.TestSpout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test to verify that tuples can be routed according to custom logic. Specific tests
 * should extend this and override the initSpout(..), initBoltA(..) and/or initBoltB(..) methods as
 * necessary to achieve the desired routing logic.
 */
public abstract class AbstractTupleRoutingTest {
  private volatile int tupleReceived;
  private volatile StringBuilder groupingInitInfo;
  private CountDownLatch outStreamQueueOfferLatch;
  private ExecutorTester executorTester;

  // Test component info. Topology is SPOUT -> BOLT_A -> BOLT_B
  protected enum Component {
    SPOUT("test-spout", "spout-id"),
    BOLT_A("test-bolt-a", "bolt-a-id"),
    BOLT_B("test-bolt-b", "bolt-b-id");

    private final String name;
    private final String id;

    Component(String name, String instanceId) {
      this.name = name;
      this.id = instanceId;
    }

    public String getName() {
      return name;
    }

    public String getInstanceId() {
      return id;
    }
  }

  @Before
  public void before() {
    tupleReceived = 0;
    groupingInitInfo = new StringBuilder();

    outStreamQueueOfferLatch = new CountDownLatch(1);
    executorTester = new ExecutorTester(outStreamQueueOfferLatch);
    executorTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    executorTester.stop();
  }

  String getInitInfoKey(String componentName) {
    return "routing-init-info+" + componentName;
  }

  /**
   * Test that tuple routing occurs using round robin
   */
  @Test
  public void testRoundRobinRouting() throws Exception {
    PhysicalPlanHelper physicalPlanHelper =
        new PhysicalPlanHelper(constructPhysicalPlan(), getComponentToVerify().getInstanceId());
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder()
        .setNewPhysicalPlanHelper(physicalPlanHelper)
        .build();

    executorTester.getInControlQueue().offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(
        getInitInfoKey(getComponentToVerify().getName()), groupingInitInfo);

    final int expectedTuplesValidated = 10;
    Runnable task = new Runnable() {
      @Override
      public void run() {
        HeronServerTester.await(outStreamQueueOfferLatch);
        assertNotEquals(0, executorTester.getOutStreamQueue().size());

        while (tupleReceived < expectedTuplesValidated) {
          if (executorTester.getOutStreamQueue().isEmpty()) {
            continue;
          }

          Message msg = executorTester.getOutStreamQueue().poll();
          assertTrue(msg instanceof HeronTuples.HeronTupleSet);

          HeronTuples.HeronTupleSet set = (HeronTuples.HeronTupleSet) msg;

          assertTrue(set.isInitialized());
          assertFalse(set.hasControl());
          assertTrue(set.hasData());

          HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
          assertEquals(dataTupleSet.getStream().getId(), "default");
          assertEquals(dataTupleSet.getStream().getComponentName(),
              getComponentToVerify().getName());

          for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
            List<Integer> destTaskIds = dataTuple.getDestTaskIdsList();
            assertEquals(1, destTaskIds.size());
            assertEquals((Integer) tupleReceived, destTaskIds.get(0));
            tupleReceived++;
          }
        }

        assertEquals(expectedTuplesValidated, tupleReceived);
        assertEquals(getExpectedComponentInitInfo(), groupingInitInfo.toString());
        executorTester.getTestLooper().exitLoop();
      }
    };

    executorTester.getTestLooper().addTasksOnWakeup(task);
    executorTester.getTestLooper().loop();
    assertEquals(expectedTuplesValidated, tupleReceived);
  }

  private PhysicalPlans.PhysicalPlan constructPhysicalPlan() {
    PhysicalPlans.PhysicalPlan.Builder physicalPlanBuilder
        = PhysicalPlans.PhysicalPlan.newBuilder();

    // Set topology protobuf
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    initSpout(topologyBuilder, Component.SPOUT.getName());
    initBoltA(topologyBuilder, Component.BOLT_A.getName(), Component.SPOUT.getName());
    initBoltB(topologyBuilder, Component.BOLT_B.getName(), Component.BOLT_A.getName());

    Config conf = new Config();
    conf.setTeamEmail("some-team@company.com");
    conf.setTeamName("some-team");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATMOST_ONCE);

    TopologyAPI.Topology topology = topologyBuilder.createTopology()
        .setName("topology-name")
        .setConfig(conf)
        .setState(TopologyAPI.TopologyState.RUNNING)
        .getTopology();

    physicalPlanBuilder.setTopology(topology);

    // Set instances
    int taskId = 0;
    for (Component component : Component.values()) {
      addComponent(physicalPlanBuilder, component, taskId++);
    }

    // Set stream mgr
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    physicalPlanBuilder.addStmgrs(stmgr);

    return physicalPlanBuilder.build();
  }

  private void addComponent(PhysicalPlans.PhysicalPlan.Builder builder,
                            Component component, int taskId) {
    PhysicalPlans.InstanceInfo.Builder instanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    instanceInfo.setComponentName(component.getName());
    instanceInfo.setTaskId(taskId);
    instanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder instance = PhysicalPlans.Instance.newBuilder();
    instance.setInstanceId(component.getInstanceId());
    instance.setStmgrId("stream-manager-id");
    instance.setInfo(instanceInfo);

    builder.addInstances(instance);
  }

  protected void initSpout(TopologyBuilder topologyBuilder, String spoutId) {
    topologyBuilder.setSpout(spoutId, new TestSpout(), 1);
  }

  protected void initBoltA(TopologyBuilder topologyBuilder,
                           String boltId, String upstreamComponentId) {
    topologyBuilder.setBolt(boltId, new TestBolt(), 1)
        .shuffleGrouping(upstreamComponentId);
  }

  protected void initBoltB(TopologyBuilder topologyBuilder,
                           String boltId, String upstreamComponentId) {
    topologyBuilder.setBolt(boltId, new TestBolt(), 1)
        .shuffleGrouping(upstreamComponentId);
  }

  protected abstract Component getComponentToVerify();

  protected abstract String getExpectedComponentInitInfo();
}
