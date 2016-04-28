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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
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

public class CustomGroupingTest {
  private static final String SPOUT_INSTANCE_ID = "spout-id";
  private static final String CUSTOM_GROUPING_INFO = "custom-grouping-info-in-prepare";

  private WakeableLooper testLooper;
  private SlaveLooper slaveLooper;
  private PhysicalPlans.PhysicalPlan physicalPlan;
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private Communicator<InstanceControlMsg> inControlQueue;
  private ExecutorService threadsPool;
  private Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;
  private volatile int tupleReceived;
  private volatile StringBuilder customGroupingInfoInPrepare;
  private Slave slave;

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    tupleReceived = 0;
    customGroupingInfoInPrepare = new StringBuilder();

    testLooper = new SlaveLooper();
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(testLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(testLooper, slaveLooper);

    slaveMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, testLooper);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();

    tupleReceived = 0;
    customGroupingInfoInPrepare = null;

    testLooper.exitLoop();
    slaveLooper.exitLoop();
    threadsPool.shutdownNow();

    physicalPlan = null;
    testLooper = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }

  /**
   * Test custom grouping
   */
  @Test
  public void testCustomGrouping() throws Exception {
    final MyCustomGrouping myCustomGrouping = new MyCustomGrouping();
    final String expectedCustomGroupingStringInPrepare = "test-spout+test-spout+default+[1]";

    physicalPlan = constructPhysicalPlan(myCustomGrouping);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(CUSTOM_GROUPING_INFO, customGroupingInfoInPrepare);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (outStreamQueue.size() != 0) {
            HeronTuples.HeronTupleSet set = outStreamQueue.poll();

            Assert.assertTrue(set.isInitialized());
            Assert.assertFalse(set.hasControl());
            Assert.assertTrue(set.hasData());

            HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
            Assert.assertEquals(dataTupleSet.getStream().getId(), "default");
            Assert.assertEquals(dataTupleSet.getStream().getComponentName(), "test-spout");

            for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
              List<Integer> destTaskIds = dataTuple.getDestTaskIdsList();
              Assert.assertEquals(destTaskIds.size(), 1);
              Assert.assertEquals(destTaskIds.get(0), (Integer) tupleReceived);
              tupleReceived++;
            }
          }
          if (tupleReceived == 10) {
            Assert.assertEquals(expectedCustomGroupingStringInPrepare,
                customGroupingInfoInPrepare.toString());
            testLooper.exitLoop();
            break;
          }
          SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
        }
      }
    };

    testLooper.addTasksOnWakeup(task);
    testLooper.loop();
  }

  private PhysicalPlans.PhysicalPlan constructPhysicalPlan(MyCustomGrouping myCustomGrouping) {
    PhysicalPlans.PhysicalPlan.Builder pPlan = PhysicalPlans.PhysicalPlan.newBuilder();

    // Set topology protobuf
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 1);
    // Here we need case switch to corresponding grouping
    topologyBuilder.setBolt("test-bolt", new TestBolt(), 1).
        customGrouping("test-spout", myCustomGrouping);

    Config conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setEnableAcking(false);

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    pPlan.setTopology(fTopology);

    // Set instances
    // Construct the spoutInstance
    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo.setComponentName("test-spout");
    spoutInstanceInfo.setTaskId(0);
    spoutInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance = PhysicalPlans.Instance.newBuilder();
    spoutInstance.setInstanceId("spout-id");
    spoutInstance.setStmgrId("stream-manager-id");
    spoutInstance.setInfo(spoutInstanceInfo);

    // Construct the boltInstanceInfo
    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo.setComponentName("test-bolt");
    boltInstanceInfo.setTaskId(1);
    boltInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance = PhysicalPlans.Instance.newBuilder();
    boltInstance.setInstanceId("bolt-id");
    boltInstance.setStmgrId("stream-manager-id");
    boltInstance.setInfo(boltInstanceInfo);

    pPlan.addInstances(spoutInstance);
    pPlan.addInstances(boltInstance);

    // Set stream mgr
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    pPlan.addStmgrs(stmgr);

    return pPlan.build();
  }

  private static class MyCustomGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = -4141962710451507976L;
    private volatile int emitted = 0;

    @Override
    public void prepare(
        TopologyContext context,
        String component,
        String streamId,
        List<Integer> targetTasks) {

      StringBuilder customGroupingInfoInPrepare =
          (StringBuilder) SingletonRegistry.INSTANCE.getSingleton(CUSTOM_GROUPING_INFO);
      customGroupingInfoInPrepare.append(context.getThisComponentId() + "+" + component
          + "+" + streamId + "+" + targetTasks.toString());
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
      List<Integer> res = new ArrayList<Integer>();
      res.add(emitted);
      emitted++;
      return res;
    }
  }
}
