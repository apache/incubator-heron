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

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.instance.SlaveTester;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.resource.Constants;
import org.apache.heron.resource.MockPhysicalPlansBuilder;
import org.apache.heron.resource.Test2PhaseCommitBolt;
import org.apache.heron.resource.TestSpout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatefulControlMessageTest {
  private SlaveTester slaveTester;

  @Before
  public void before() {
    slaveTester = new SlaveTester();
    slaveTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    slaveTester.stop();
  }

  @Test
  public void testPreSaveAndPostSave() throws Exception {
    CountDownLatch preSaveLatch = new CountDownLatch(1);
    CountDownLatch postSaveLatch = new CountDownLatch(1);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.PRESAVE_LATCH, preSaveLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.POSTSAVE_LATCH, postSaveLatch);

    // initially non of preSave or postSave are invoked yet
    slaveTester.getInControlQueue().offer(buildRestoreInstanceState("c0"));
    slaveTester.getInControlQueue().offer(buildStartInstanceProcessingMessage("c0"));
    slaveTester.getInControlQueue().offer(buildPhysicalPlanMessage());

    assertEquals(1, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());

    // this should invoke preSave
    slaveTester.getInStreamQueue().offer(buildPersistStateMessage("c0"));
    assertTrue(preSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(1, postSaveLatch.getCount());

    // this should invoke postSave
    slaveTester.getInControlQueue().offer(buildCheckpointSavedMessage("c0"));
    assertTrue(postSaveLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));
    assertEquals(0, preSaveLatch.getCount());
    assertEquals(0, postSaveLatch.getCount());
  }

  private Message buildPersistStateMessage(String checkpointId) {
    CheckpointManager.InitiateStatefulCheckpoint.Builder builder = CheckpointManager
        .InitiateStatefulCheckpoint
        .newBuilder();

    builder.setCheckpointId(checkpointId);

    return builder.build();
  }

  private InstanceControlMsg buildRestoreInstanceState(String checkpointId) {
    return InstanceControlMsg.newBuilder()
        .setRestoreInstanceStateRequest(
            CheckpointManager.RestoreInstanceStateRequest
                .newBuilder()
                .setState(CheckpointManager.InstanceStateCheckpoint
                    .newBuilder()
                    .setCheckpointId(checkpointId))
                .build()
        )
        .build();
  }

  private InstanceControlMsg buildStartInstanceProcessingMessage(String checkpointId) {
    return InstanceControlMsg.newBuilder()
        .setStartInstanceStatefulProcessing(
            CheckpointManager.StartInstanceStatefulProcessing
                .newBuilder()
                .setCheckpointId(checkpointId)
                .build()
        )
        .build();
  }

  private InstanceControlMsg buildCheckpointSavedMessage(String checkpointId) {
    CheckpointManager.StatefulConsistentCheckpointSaved.Builder builder = CheckpointManager
        .StatefulConsistentCheckpointSaved
        .newBuilder();

    CheckpointManager.StatefulConsistentCheckpoint.Builder ckptBuilder = CheckpointManager
        .StatefulConsistentCheckpoint
        .newBuilder();

    ckptBuilder.setCheckpointId(checkpointId);
    ckptBuilder.setPackingPlanId("p0");

    builder.setConsistentCheckpoint(ckptBuilder.build());

    return InstanceControlMsg.newBuilder()
        .setStatefulCheckpointSaved(builder.build())
        .build();
  }

  private InstanceControlMsg buildPhysicalPlanMessage() {
    return InstanceControlMsg.newBuilder()
        .setNewPhysicalPlanHelper(getPhysicalPlan())
        .build();
  }

  private PhysicalPlanHelper getPhysicalPlan() {
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
                new Test2PhaseCommitBolt()
            )
            .build();

    return new PhysicalPlanHelper(physicalPlan, "bolt-id");
  }
}
