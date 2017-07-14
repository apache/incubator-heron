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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.JavaSerializer;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.testhelpers.HeronServerTester;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.SlaveTester;
import com.twitter.heron.proto.system.HeronTuples;
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
  private static final int SRC_TASK_ID = 1;
  private static IPluggableSerializer serializer = new JavaSerializer();

  // Singleton to be changed globally for testing
  private AtomicInteger ackCount;
  private AtomicInteger failCount;
  private AtomicInteger tupleExecutedCount;
  private volatile StringBuilder receivedStrings;

  private SlaveTester slaveTester;

  static {
    serializer.initialize(null);
  }

  @Before
  public void before() {
    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);
    tupleExecutedCount = new AtomicInteger(0);
    receivedStrings = new StringBuilder();

    slaveTester = new SlaveTester();
    slaveTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    slaveTester.stop();
  }

  /**
   * Test the reading of a tuple and apply execute on that tuple
   */
  @Test
  public void testReadTupleAndExecute() {
    PhysicalPlans.PhysicalPlan physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, BOLT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    slaveTester.getInControlQueue().offer(instanceControlMsg);

    final int expectedTuples = 10;
    CountDownLatch executeLatch = new CountDownLatch(expectedTuples);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.EXECUTE_COUNT, tupleExecutedCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.EXECUTE_LATCH, executeLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.RECEIVED_STRING_LIST, receivedStrings);

    // Send tuples to bolt instance
    HeronTuples.HeronTupleSet.Builder heronTupleSet = HeronTuples.HeronTupleSet.newBuilder();
    heronTupleSet.setSrcTaskId(SRC_TASK_ID);
    HeronTuples.HeronDataTupleSet.Builder dataTupleSet = HeronTuples.HeronDataTupleSet.newBuilder();
    TopologyAPI.StreamId.Builder streamId = TopologyAPI.StreamId.newBuilder();
    streamId.setComponentName("test-spout");
    streamId.setId("default");
    dataTupleSet.setStream(streamId);

    // We will add 10 tuples to the set
    for (int i = 0; i < expectedTuples; i++) {
      HeronTuples.HeronDataTuple.Builder dataTuple = HeronTuples.HeronDataTuple.newBuilder();
      dataTuple.setKey(19901017 + i);

      HeronTuples.RootId.Builder rootId = HeronTuples.RootId.newBuilder();
      rootId.setKey(19901017 + i);
      rootId.setTaskid(0);
      dataTuple.addRoots(rootId);

      String tupleValue = (i & 1) == 0 ? "A" : "B";
      dataTuple.addValues(ByteString.copyFrom(serializer.serialize(tupleValue)));

      dataTupleSet.addTuples(dataTuple);
    }

    heronTupleSet.setData(dataTupleSet);
    slaveTester.getInStreamQueue().offer(heronTupleSet.build());

    // Wait the bolt's finishing
    HeronServerTester.await(executeLatch);
    Assert.assertEquals(expectedTuples, tupleExecutedCount.intValue());
    Assert.assertEquals(expectedTuples / 2, ackCount.intValue());
    Assert.assertEquals(expectedTuples / 2, failCount.intValue());
    Assert.assertEquals("ABABABABAB", receivedStrings.toString());
  }
}
