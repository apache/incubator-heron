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

package com.twitter.heron.instance.bolt;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.KryoSerializer;
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
import com.twitter.heron.resource.UnitTestHelper;

/**
 * To test the Bolt's ReadTupleAndExecute() method, it will:
 * 1. We will instantiate a slave with TestBolt's instance.
 * 2. Construct a bunch of mock Tuples as protobuf Message to be consumed by the TestBolt
 * 3. Offer those protobuf Message into inStreamQueue.
 * 4. The TestBolt should consume the Tuples, and behave as described in its comments.
 * 5. Check the values in singleton registry: execute-count, ack-count, fail-count, received-string-list.
 * 6. We will also check some common values, for instance, the stream name.
 */
public class BoltInstanceTest {
  private static final String BOLT_INSTANCE_ID = "bolt-id";
  private static IPluggableSerializer serializer;

  private WakeableLooper testLooper;
  private SlaveLooper slaveLooper;

  // Singleton to be changed globally for testing
  private AtomicInteger ackCount;
  private AtomicInteger failCount;
  private AtomicInteger tupleExecutedCount;
  private volatile StringBuilder receivedStrings;
  private PhysicalPlans.PhysicalPlan physicalPlan;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private Communicator<Message> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private Communicator<Message> inStreamQueue;
  private Communicator<InstanceControlMsg> inControlQueue;
  private ExecutorService threadsPool;
  private Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;
  private Slave slave;

  @BeforeClass
  public static void beforeClass() throws Exception {
    serializer = new KryoSerializer();
    serializer.initialize(null);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    serializer = null;
  }

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);
    tupleExecutedCount = new AtomicInteger(0);
    receivedStrings = new StringBuilder();

    testLooper = new SlaveLooper();
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<Message>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<Message>(testLooper, slaveLooper);
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

    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);
    tupleExecutedCount = new AtomicInteger(0);
    receivedStrings = null;

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
   * Test the reading of a tuple and apply execute on that tuple
   */
  @Test
  public void testReadTupleAndExecute() throws Exception {
    physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, BOLT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);
    SingletonRegistry.INSTANCE.registerSingleton("execute-count", tupleExecutedCount);
    SingletonRegistry.INSTANCE.registerSingleton("received-string-list", receivedStrings);

    // Send tuples to bolt instance
    HeronTuples.HeronTupleSet.Builder heronTupleSet = HeronTuples.HeronTupleSet.newBuilder();
    HeronTuples.HeronDataTupleSet.Builder dataTupleSet = HeronTuples.HeronDataTupleSet.newBuilder();
    TopologyAPI.StreamId.Builder streamId = TopologyAPI.StreamId.newBuilder();
    streamId.setComponentName("test-spout");
    streamId.setId("default");
    dataTupleSet.setStream(streamId);

    // We will add 10 tuples to the set
    for (int i = 0; i < 10; i++) {
      HeronTuples.HeronDataTuple.Builder dataTuple = HeronTuples.HeronDataTuple.newBuilder();
      dataTuple.setKey(19901017 + i);

      HeronTuples.RootId.Builder rootId = HeronTuples.RootId.newBuilder();
      rootId.setKey(19901017 + i);
      rootId.setTaskid(0);
      dataTuple.addRoots(rootId);

      String s = "";
      if ((i & 1) == 0) {
        s = "A";
      } else {
        s = "B";
      }
      ByteString byteString = ByteString.copyFrom(serializer.serialize(s));
      dataTuple.addValues(byteString);

      dataTupleSet.addTuples(dataTuple);
    }

    heronTupleSet.setData(dataTupleSet);
    inStreamQueue.offer(heronTupleSet.build());

    for (int i = 0; i < Constants.RETRY_TIMES; i++) {
      if (tupleExecutedCount.intValue() == 10) {
        break;
      }
      SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
    }

    // Wait the bolt's finishing
    SysUtils.sleep(Constants.TEST_WAIT_TIME_MS);
    Assert.assertEquals(10, tupleExecutedCount.intValue());
    Assert.assertEquals(5, ackCount.intValue());
    Assert.assertEquals(5, failCount.intValue());
    Assert.assertEquals("ABABABABAB", receivedStrings.toString());
  }
}
