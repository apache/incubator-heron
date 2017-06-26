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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.JavaSerializer;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.network.HeronServerTester;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.SlaveTester;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;


/**
 * To test the SpoutInstance.
 * We will test by instantiate a slave with TestSpout's instance:
 * 1. nextTuple().
 * Check whether Message inside slaveTester.getOutStreamQueue() matches tuples emitted by TestSpout.
 * We will not enable acking system and not enable timeout.
 * 2. gatherMetrics()
 * We wait for the interval for gathering metrics, and check whether the Metrics Message contains
 * the expected Metrics
 * 3. doImmediateAcks()
 * We do immediate acks, and check whether the singleton Constants.ACK_COUNT matches expected value.
 * We will not enable acking system and not enable timeout.
 * 4. lookForTimeouts()
 * We enable acking system and timeout but not ack any tuples, and check whether it will be timeout automatically
 * and calling fail(), by checking whether the singleton Constants.FAIL_COUNT matches expected value
 * 5. ackAndFail()
 * We enable acking system without timeout, and let the spout emit tuples; then we collect the corresponding tuples'
 * info, for instance: key, root and so on. Then we construct 5 acks and 5 fails and send back.
 * We will check whether the singleton Constants.ACK_COUNT and "fail count" match the expected value.
 */
public class SpoutInstanceTest {
  private static final String SPOUT_INSTANCE_ID = "spout-id";
  private static final int SRC_TASK_ID = 1;
  private static IPluggableSerializer serializer = new JavaSerializer();

  // Singleton to be changed globally for testing
  private AtomicInteger ackCount;
  private AtomicInteger failCount;

  private SlaveTester slaveTester;

  private int tupleReceived;
  private List<HeronTuples.HeronDataTuple> heronDataTupleList;

  static {
    serializer.initialize(null);
  }

  @Before
  public void before() {
    tupleReceived = 0;
    heronDataTupleList = new ArrayList<>();
    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);

    slaveTester = new SlaveTester();
    slaveTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    slaveTester.stop();
  }

  /**
   * Test the fetching of next tuple
   */
  @Test
  public void testNextTuple() {
    initSpout(slaveTester, false, -1);

    Runnable task = new Runnable() {
      private String streamId = "";
      private String componentName = "";
      private String receivedTupleStrings = "";

      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (slaveTester.getOutStreamQueue().size() != 0) {
            HeronTuples.HeronTupleSet set = slaveTester.getOutStreamQueue().poll();

            Assert.assertTrue(set.isInitialized());
            Assert.assertFalse(set.hasControl());
            Assert.assertTrue(set.hasData());

            HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
            streamId = dataTupleSet.getStream().getId();
            componentName = dataTupleSet.getStream().getComponentName();
            Assert.assertEquals(streamId, "default");
            Assert.assertEquals(componentName, "test-spout");

            Assert.assertEquals(streamId, "default");
            Assert.assertEquals(componentName, "test-spout");

            for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
              for (ByteString b : dataTuple.getValuesList()) {
                receivedTupleStrings += serializer.deserialize(b.toByteArray());
              }
            }
            tupleReceived += dataTupleSet.getTuplesCount();
          }
          if (tupleReceived == 10) {
            Assert.assertEquals("ABABABABAB", receivedTupleStrings);
            slaveTester.getTestLooper().exitLoop();
            break;
          }
        }
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();

    Assert.assertEquals(tupleReceived, 10);
  }

  /**
   * Test the gathering of metrics
   */
  @Test
  public void testGatherMetrics() {
    initSpout(slaveTester, false, -1);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (!slaveTester.getSlaveMetricsOut().isEmpty()) {
            Metrics.MetricPublisherPublishMessage msg = slaveTester.getSlaveMetricsOut().poll();
            Set<String> metricsName = new HashSet<>();
            for (Metrics.MetricDatum metricDatum : msg.getMetricsList()) {
              metricsName.add(metricDatum.getName());
            }

            Assert.assertTrue(metricsName.contains("__ack-count/default"));
            Assert.assertTrue(metricsName.contains("__complete-latency/default"));
            Assert.assertTrue(metricsName.contains("__emit-count/default"));
            Assert.assertTrue(metricsName.contains("__next-tuple-latency"));
            Assert.assertTrue(metricsName.contains("__next-tuple-count"));

            slaveTester.getTestLooper().exitLoop();
            break;
          }
        }
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();
  }

  /**
   * Test with the acking immediately
   */
  @Test
  public void testDoImmediateAcks() {
    final int tuplesExpected = 10;
    final CountDownLatch ackLatch = new CountDownLatch(tuplesExpected);

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_LATCH, ackLatch);

    initSpout(slaveTester, false, -1);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        drainOutStream();
        if (tupleReceived == 10) {
          // Wait until the acks are received
          HeronServerTester.await(ackLatch);
          Assert.assertEquals(tuplesExpected, ackCount.intValue());
          slaveTester.getTestLooper().exitLoop();
        }
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();
  }

  @Test
  public void testLookForTimeouts() {
    final int tuplesExpected = 10;
    final CountDownLatch failLatch = new CountDownLatch(tuplesExpected);

    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_LATCH, failLatch);

    initSpout(slaveTester, true, 1);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        // Wait until the fails are received
        HeronServerTester.await(failLatch);

        Assert.assertEquals(tuplesExpected, failCount.intValue());
        slaveTester.getTestLooper().exitLoop();
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();
  }

  /**
   * We will receive tuples and then send back the corresponding ack&amp;fail tuples
   */

  @Test
  public void testAckAndFail() {
    final int failsExpected = 5;
    final int acksExpected = 5;
    final CountDownLatch failLatch = new CountDownLatch(failsExpected);
    final CountDownLatch ackLatch = new CountDownLatch(acksExpected);

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_LATCH, ackLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_LATCH, failLatch);

    initSpout(slaveTester, true, -1);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        drainOutStream();
        if (tupleReceived == acksExpected + failsExpected) {
          constructAndSendAcks();

          // Wait until the fails and acks are received
          HeronServerTester.await(failLatch);
          HeronServerTester.await(ackLatch);

          Assert.assertEquals(acksExpected, ackCount.intValue());
          Assert.assertEquals(failsExpected, failCount.intValue());
          slaveTester.getTestLooper().exitLoop();
        }
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();
  }

  private void constructAndSendAcks() {
    // We will construct the ack&fail tuples
    // We will construct 5 acks and 5 fails
    HeronTuples.HeronTupleSet.Builder bldr = HeronTuples.HeronTupleSet.newBuilder();
    bldr.setSrcTaskId(SRC_TASK_ID);
    HeronTuples.HeronControlTupleSet.Builder controlTupleSet
        = HeronTuples.HeronControlTupleSet.newBuilder();

    for (int i = 0; i < heronDataTupleList.size() / 2; i++) {
      HeronTuples.AckTuple.Builder ackBuilder = HeronTuples.AckTuple.newBuilder();
      ackBuilder.setAckedtuple(heronDataTupleList.get(i).getKey());

      for (HeronTuples.RootId rt : heronDataTupleList.get(i).getRootsList()) {
        ackBuilder.addRoots(rt);
      }
      controlTupleSet.addAcks(ackBuilder);
    }

    for (int i = heronDataTupleList.size() / 2; i < heronDataTupleList.size(); i++) {
      HeronTuples.AckTuple.Builder ackBuilder = HeronTuples.AckTuple.newBuilder();
      ackBuilder.setAckedtuple(heronDataTupleList.get(i).getKey());

      for (HeronTuples.RootId rt : heronDataTupleList.get(i).getRootsList()) {
        ackBuilder.addRoots(rt);
      }
      controlTupleSet.addFails(ackBuilder);
    }

    bldr.setControl(controlTupleSet);

    // We will send back to the SpoutInstance
    slaveTester.getInStreamQueue().offer(bldr.build());
  }

  private void drainOutStream() {
    while (slaveTester.getOutStreamQueue().size() != 0) {
      HeronTuples.HeronTupleSet set = slaveTester.getOutStreamQueue().poll();

      Assert.assertTrue(set.isInitialized());
      Assert.assertTrue(set.hasData());

      HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();

      tupleReceived += dataTupleSet.getTuplesCount();
      heronDataTupleList.addAll(dataTupleSet.getTuplesList());
    }
  }

  private static void initSpout(SlaveTester slaveTester, boolean ackEnabled, int timeout) {
    PhysicalPlans.PhysicalPlan physicalPlan = UnitTestHelper.getPhysicalPlan(ackEnabled, timeout);
    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);

    slaveTester.getInControlQueue().offer(
        InstanceControlMsg.newBuilder().setNewPhysicalPlanHelper(physicalPlanHelper).build());
  }
}
