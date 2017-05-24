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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.JavaSerializer;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
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
  private static IPluggableSerializer serializer = new JavaSerializer();

  // Singleton to be changed globally for testing
  private AtomicInteger ackCount;
  private AtomicInteger failCount;
  private PhysicalPlans.PhysicalPlan physicalPlan;

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
    physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    slaveTester.getInControlQueue().offer(instanceControlMsg);

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
          SysUtils.sleep(Constants.RETRY_INTERVAL);
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
    physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    slaveTester.getInControlQueue().offer(instanceControlMsg);

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
          SysUtils.sleep(Constants.RETRY_INTERVAL);
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
    physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);

    slaveTester.getInControlQueue().offer(instanceControlMsg);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        drainOutStream();
        if (tupleReceived == 10) {
          // We fetch it from SingletonRegistry
          for (int i = 0; i < Constants.RETRY_TIMES; i++) {
            if (ackCount.intValue() != 0) {
              break;
            }
            SysUtils.sleep(Constants.RETRY_INTERVAL);
          }

          // Wait the bolt's finishing
          SysUtils.sleep(Constants.TEST_WAIT_TIME);
          Assert.assertEquals(10, ackCount.intValue());
          slaveTester.getTestLooper().exitLoop();
        }
      }
    };

    slaveTester.getTestLooper().addTasksOnWakeup(task);
    slaveTester.getTestLooper().loop();
  }

  @Test
  public void testLookForTimeouts() {
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, 1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);

    slaveTester.getInControlQueue().offer(instanceControlMsg);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (failCount.intValue() != 0) {
            break;
          }
          SysUtils.sleep(Constants.RETRY_INTERVAL);
        }

        // Wait the bolt's finishing
        SysUtils.sleep(Constants.TEST_WAIT_TIME);
        Assert.assertEquals(10, failCount.intValue());
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
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);

    slaveTester.getInControlQueue().offer(instanceControlMsg);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        drainOutStream();
        if (tupleReceived == 10) {
          constructAndSendAcks();
          // We fetch it from SingletonRegistry
          for (int i = 0; i < Constants.RETRY_TIMES; i++) {
            if (ackCount.intValue() != 0 || failCount.intValue() != 0) {
              break;
            }
            SysUtils.sleep(Constants.RETRY_INTERVAL);
          }

          // Wait the bolt's finishing
          SysUtils.sleep(Constants.TEST_WAIT_TIME);
          Assert.assertEquals(5, ackCount.intValue());
          Assert.assertEquals(5, failCount.intValue());
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
}
