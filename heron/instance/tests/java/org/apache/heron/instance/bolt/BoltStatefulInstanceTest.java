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
package org.apache.heron.instance.bolt;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.serializer.JavaSerializer;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.ExecutorTester;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.proto.system.HeronTuples;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.resource.Constants;
import org.apache.heron.resource.MockPhysicalPlansBuilder;
import org.apache.heron.resource.TestSpout;
import org.apache.heron.resource.TestStatefulBolt;
import org.apache.heron.resource.TestTwoPhaseStatefulBolt;
import org.apache.heron.resource.UnitTestHelper;

import static org.junit.Assert.*;

/**
 * Test if stateful bolt is able to respond to incoming control/data tuples as expected.
 */
public class BoltStatefulInstanceTest {
  private ExecutorTester executorTester;
  private static IPluggableSerializer serializer = new JavaSerializer();

  @Before
  public void before() {
    executorTester = new ExecutorTester();
    executorTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    executorTester.stop();
  }

  @Test
  public void testPreSaveAndPostSave() throws Exception {
    CountDownLatch preSaveLatch = new CountDownLatch(1);
    CountDownLatch postSaveLatch = new CountDownLatch(1);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.PRESAVE_LATCH, preSaveLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.POSTSAVE_LATCH, postSaveLatch);

    executorTester.getInControlQueue().offer(UnitTestHelper.buildRestoreInstanceState("c0"));
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildStartInstanceProcessingMessage("c0"));
    executorTester.getInControlQueue().offer(buildPhysicalPlanMessageFor2PCBolt());

    // initially non of preSave or postSave are invoked yet
    assertEquals(1, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());

    // this should invoke preSave
    executorTester.getInStreamQueue().offer(UnitTestHelper.buildPersistStateMessage("c0"));
    assertTrue(preSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());

    // this should invoke postSave
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildCheckpointSavedMessage("c0", "p0"));
    assertTrue(postSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(0, postSaveLatch.getCount());
  }

  @Test
  public void testPreRestore() throws InterruptedException {
    CountDownLatch preRestoreLatch = new CountDownLatch(1);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.PRERESTORE_LATCH, preRestoreLatch);

    executorTester.getInControlQueue().offer(UnitTestHelper.buildRestoreInstanceState("c0"));
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildStartInstanceProcessingMessage("c0"));
    executorTester.getInControlQueue().offer(buildPhysicalPlanMessageFor2PCBolt());

    assertEquals(1, preRestoreLatch.getCount());

    executorTester.getInControlQueue().offer(UnitTestHelper.buildRestoreInstanceState("cx"));

    assertTrue(preRestoreLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preRestoreLatch.getCount());
  }

  /**
   * Ensure that for ITwoPhaseStatefulComponent bolts, after a preSave, execute will not be invoked
   * unless the corresponding postSave is called.
   */
  @Test
  public void testPostSaveBlockExecute() throws Exception {
    CountDownLatch preSaveLatch = new CountDownLatch(1);
    CountDownLatch postSaveLatch = new CountDownLatch(1);

    CountDownLatch executeLatch = new CountDownLatch(1); // expect to execute one tuple

    SingletonRegistry.INSTANCE.registerSingleton(Constants.PRESAVE_LATCH, preSaveLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.POSTSAVE_LATCH, postSaveLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.EXECUTE_LATCH, executeLatch);

    executorTester.getInControlQueue().offer(UnitTestHelper.buildRestoreInstanceState("c0"));
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildStartInstanceProcessingMessage("c0"));
    executorTester.getInControlQueue().offer(buildPhysicalPlanMessageFor2PCBolt());

    // initially non of preSave or postSave are invoked yet
    assertEquals(1, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());
    assertEquals(1, executeLatch.getCount());

    // this should invoke preSave
    executorTester.getInStreamQueue().offer(UnitTestHelper.buildPersistStateMessage("c0"));

    // put a data tuple into the inStreamQueue
    executorTester.getInStreamQueue().offer(buildTupleSet());

    assertTrue(preSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());
    assertEquals(1, executeLatch.getCount());

    // Wait for a bounded amount of time, assert that the tuple will not execute as it is
    // blocked on postSave. This is because we only want to allow one uncommitted "transaction" on
    // each task. See the design doc for more details.
    assertFalse(executeLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());
    assertEquals(1, executeLatch.getCount());

    // this should invoke postSave
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildCheckpointSavedMessage("c0", "p0"));
    assertTrue(postSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertTrue(executeLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));

    assertEquals(0, preSaveLatch.getCount());
    assertEquals(0, postSaveLatch.getCount());
    assertEquals(0, executeLatch.getCount());
  }

  /**
   * Ensure that the aforementioned behaviour does not apply for bolts that don't implement
   * ITwoPhaseStatefulComponent
   */
  @Test
  public void testExecuteNotBlocked() throws Exception {
    CountDownLatch preSaveLatch = new CountDownLatch(1);
    CountDownLatch executeLatch = new CountDownLatch(1); // expect to execute one tuple

    SingletonRegistry.INSTANCE.registerSingleton(Constants.PRESAVE_LATCH, preSaveLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.EXECUTE_LATCH, executeLatch);

    executorTester.getInControlQueue().offer(UnitTestHelper.buildRestoreInstanceState("c0"));
    executorTester.getInControlQueue().offer(
        UnitTestHelper.buildStartInstanceProcessingMessage("c0"));
    executorTester.getInControlQueue().offer(buildPhysicalPlanMessageForStatefulBolt());

    // initially non of preSave or postSave are invoked yet
    assertEquals(1, preSaveLatch.getCount());
    assertEquals(1, executeLatch.getCount());

    // this should invoke preSave
    executorTester.getInStreamQueue().offer(UnitTestHelper.buildPersistStateMessage("c0"));

    // put a data tuple into the inStreamQueue
    executorTester.getInStreamQueue().offer(buildTupleSet());

    assertTrue(preSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());

    // no need to wait for postSave as the bolt doesn't implement ITwoPhaseStatefulComponent
    assertTrue(executeLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(0, executeLatch.getCount());
  }

  // build a tuple set that contains one data tuple
  private HeronTuples.HeronTupleSet buildTupleSet() {
    HeronTuples.HeronTupleSet.Builder heronTupleSet = HeronTuples.HeronTupleSet.newBuilder();
    heronTupleSet.setSrcTaskId(1);
    HeronTuples.HeronDataTupleSet.Builder dataTupleSet = HeronTuples.HeronDataTupleSet.newBuilder();
    TopologyAPI.StreamId.Builder streamId = TopologyAPI.StreamId.newBuilder();
    streamId.setComponentName("test-spout");
    streamId.setId("default");
    dataTupleSet.setStream(streamId);

    HeronTuples.HeronDataTuple.Builder dataTuple = HeronTuples.HeronDataTuple.newBuilder();
    dataTuple.setKey(0);

    HeronTuples.RootId.Builder rootId = HeronTuples.RootId.newBuilder();
    rootId.setKey(0);
    rootId.setTaskid(0);
    dataTuple.addRoots(rootId);

    dataTuple.addValues(ByteString.copyFrom(serializer.serialize("A")));
    dataTupleSet.addTuples(dataTuple);
    heronTupleSet.setData(dataTupleSet);

    return heronTupleSet.build();
  }

  private InstanceControlMsg buildPhysicalPlanMessageFor2PCBolt() {
    return buildPhysicalPlanMessage(new TestTwoPhaseStatefulBolt());
  }

  private InstanceControlMsg buildPhysicalPlanMessageForStatefulBolt() {
    return buildPhysicalPlanMessage(new TestStatefulBolt());
  }

  private InstanceControlMsg buildPhysicalPlanMessage(IRichBolt bolt) {
    PhysicalPlans.PhysicalPlan physicalPlan =
        MockPhysicalPlansBuilder
            .newBuilder()
            .withTopologyConfig(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE, -1)
            .withTopologyState(TopologyAPI.TopologyState.RUNNING)
            .withSpoutInstance(
                "test-spout",
                0,
                "spout-id",
                new TestSpout()
            )
            .withBoltInstance(
                "test-bolt",
                1,
                "bolt-id",
                "test-spout",
                bolt
            )
            .build();

    PhysicalPlanHelper ph = new PhysicalPlanHelper(physicalPlan, "bolt-id");

    return InstanceControlMsg.newBuilder()
        .setNewPhysicalPlanHelper(ph)
        .build();
  }
}
