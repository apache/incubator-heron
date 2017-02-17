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

package com.twitter.heron.instance.spout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.Slave;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

public class ActivateDeactivateTest {
  private static final String SPOUT_INSTANCE_ID = "spout-id";
  private SlaveLooper slaveLooper;

  // Singleton to be changed globally for testing
  private PhysicalPlans.PhysicalPlan physicalPlan;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private Communicator<InstanceControlMsg> inControlQueue;
  private ExecutorService threadsPool;
  private Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;
  private Slave slave;

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, null);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(null, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    slaveMetricsOut = new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, null);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(null, slaveLooper);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();
    if (slaveLooper != null) {
      slaveLooper.exitLoop();
    }
    if (threadsPool != null) {
      threadsPool.shutdownNow();
    }

    physicalPlan = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }


  /**
   * We will test whether spout would pull activate/deactivate state change and
   * invoke activate()/deactivate()
   */

  @Test
  public void testActivateAndDeactivate() throws Exception {
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.RUNNING);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    AtomicInteger activateCount = new AtomicInteger(0);
    AtomicInteger deactivateCount = new AtomicInteger(0);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACTIVATE_COUNT, activateCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.DEACTIVATE_COUNT, deactivateCount);

    // Now the activateCount and deactivateCount should be 0
    // And we start the test
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.PAUSED);
    physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    Thread.sleep(Constants.TEST_WAIT_TIME_MS);

    Assert.assertEquals(1, deactivateCount.get());

    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.RUNNING);
    physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    Thread.sleep(Constants.TEST_WAIT_TIME_MS);

    Assert.assertEquals(1, activateCount.get());
    Assert.assertEquals(1, deactivateCount.get());
  }
}
