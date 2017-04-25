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

package com.twitter.heron.grouping;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.Slave;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.TestBolt;
import com.twitter.heron.resource.TestSpout;
import com.twitter.heron.resource.UnitTestHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test to verify that tuples can be routed according to custom logic. Specific tests
 * should extend this and override the initSpout(..), initBoltA(..) and/or initBoltB(..) methods as
 * necessary to achieve the desired routing logic.
 */
public abstract class AbstractTupleRoutingTest {
  private WakeableLooper testLooper;
  private SlaveLooper slaveLooper;
  private PhysicalPlans.PhysicalPlan physicalPlan;
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private Communicator<InstanceControlMsg> inControlQueue;
  private ExecutorService threadsPool;
  private volatile int tupleReceived;
  private volatile StringBuilder groupingInitInfo;
  private Slave slave;

  // Test component info. Topology is SPOUT -> BOLT_A -> BOLT_B
  protected enum Component {
    SPOUT("test-spout", "spout-id"),
    BOLT_A("test-bolt-a", "bolt-a-id"),
    BOLT_B("test-bolt-b", "bolt-b-id");

    private String name;
    private String id;

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
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    tupleReceived = 0;
    groupingInitInfo = new StringBuilder();

    testLooper = new SlaveLooper();
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<>(testLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<>(testLooper, slaveLooper);

    Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut =
        new Communicator<>(slaveLooper, testLooper);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();

    tupleReceived = 0;
    groupingInitInfo = null;

    if (testLooper != null) {
      testLooper.exitLoop();
    }
    if (slaveLooper != null) {
      slaveLooper.exitLoop();
    }
    if (threadsPool != null) {
      threadsPool.shutdownNow();
    }
    physicalPlan = null;
    testLooper = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }

  protected String getInitInfoKey(String componentName) {
    return "routing-init-info+" + componentName;
  }

  /**
   * Test that tuple routing occurs using round robin
   */
  @Test
  public void testRoundRobinRouting() throws Exception {
    physicalPlan = constructPhysicalPlan();

    PhysicalPlanHelper physicalPlanHelper =
        new PhysicalPlanHelper(physicalPlan, getComponentToVerify().getInstanceId());
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder()
        .setNewPhysicalPlanHelper(physicalPlanHelper)
        .build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(
        getInitInfoKey(getComponentToVerify().getName()), groupingInitInfo);

    final int expectedTuplesValidated = 10;
    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (outStreamQueue.size() != 0) {
            HeronTuples.HeronTupleSet set = outStreamQueue.poll();

            assertTrue(set.isInitialized());
            assertFalse(set.hasControl());
            assertTrue(set.hasData());

            HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
            assertEquals(dataTupleSet.getStream().getId(), "default");
            assertEquals(dataTupleSet.getStream().getComponentName(),
                getComponentToVerify().getName());

            for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
              List<Integer> destTaskIds = dataTuple.getDestTaskIdsList();
              assertEquals(destTaskIds.size(), 1);
              assertEquals(destTaskIds.get(0), (Integer) tupleReceived);
              tupleReceived++;
            }
          }
          if (tupleReceived == expectedTuplesValidated) {
            assertEquals(getExpectedComponentInitInfo(), groupingInitInfo.toString());
            testLooper.exitLoop();
            break;
          }
          SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
        }
      }
    };

    testLooper.addTasksOnWakeup(task);
    testLooper.loop();
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
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setEnableAcking(false);

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
