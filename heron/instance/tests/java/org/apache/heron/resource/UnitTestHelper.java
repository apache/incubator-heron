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

package org.apache.heron.resource;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Map;

import com.google.protobuf.Message;

import org.junit.Ignore;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.config.SystemConfigKey;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.stmgr.StreamManager;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.PhysicalPlans;

/**
 * A helper used for unit test, it provides following util methods:
 * 1. Get a mock physical plan
 * 2. Clear the singleton registry by using reflection
 * 3. Get a RegisterInstanceResponse, used by a mock stream manager to send back to instance when it
 * receives a RegisterInstanceRequest
 */

@Ignore
public final class UnitTestHelper {

  private UnitTestHelper() {
  }

  /**
   * Construct a physical plan with basic setting.
   *
   * @param ackEnabled whether the acking system is enabled
   * @param messageTimeout the seconds for a tuple to be time-out. -1 means the timeout is not enabled.
   * @param topologyState the Topology State inside this PhysicalPlan, for instance, RUNNING.
   * @return the corresponding Physical Plan
   */
  public static PhysicalPlans.PhysicalPlan getPhysicalPlan(
      boolean ackEnabled,
      int messageTimeout,
      TopologyAPI.TopologyState topologyState
  ) {
    Config.TopologyReliabilityMode reliabilityMode = ackEnabled
        ? Config.TopologyReliabilityMode.ATLEAST_ONCE
        : Config.TopologyReliabilityMode.ATMOST_ONCE;

    return MockPhysicalPlansBuilder
        .newBuilder()
        .withTopologyConfig(reliabilityMode, messageTimeout)
        .withTopologyState(topologyState)
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
            new TestBolt()
        )
        .build();
  }

  public static PhysicalPlans.PhysicalPlan getPhysicalPlan(boolean ackEnabled, int messageTimeout) {
    return getPhysicalPlan(ackEnabled, messageTimeout, TopologyAPI.TopologyState.RUNNING);
  }

  @SuppressWarnings("unchecked")
  public static void clearSingletonRegistry() throws IllegalAccessException, NoSuchFieldException {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  public static PhysicalPlans.Instance getInstance(String instanceId) {
    int taskId = 0;
    int componentIndex = 0;
    String componentName = "component_name";

    String streamId = "stream_id";
    // Create the protobuf Instance
    PhysicalPlans.InstanceInfo instanceInfo = PhysicalPlans.InstanceInfo.newBuilder().
        setTaskId(taskId).setComponentIndex(componentIndex).setComponentName(componentName).build();

    PhysicalPlans.Instance instance = PhysicalPlans.Instance.newBuilder().
        setInstanceId(instanceId).setStmgrId(streamId).setInfo(instanceInfo).build();

    return instance;
  }

  public static void addSystemConfigToSingleton() {
    String runFiles = System.getenv(Constants.BUILD_TEST_SRCDIR);
    if (runFiles == null) {
      throw new RuntimeException("Failed to fetch run files resources from built jar");
    }

    String filePath =
        Paths.get(runFiles, Constants.BUILD_TEST_HERON_INTERNALS_CONFIG_PATH).toString();
    SystemConfig.Builder sb = SystemConfig.newBuilder(true)
        .putAll(filePath, true)
        .put(SystemConfigKey.HERON_METRICS_EXPORT_INTERVAL, 1);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.HERON_SYSTEM_CONFIG, sb.build());
  }

  public static StreamManager.RegisterInstanceResponse getRegisterInstanceResponse() {
    StreamManager.RegisterInstanceResponse.Builder registerInstanceResponse =
        StreamManager.RegisterInstanceResponse.newBuilder();
    registerInstanceResponse.setPplan(getPhysicalPlan(false, -1));
    Common.Status.Builder status = Common.Status.newBuilder();
    status.setStatus(Common.StatusCode.OK);
    registerInstanceResponse.setStatus(status);

    return registerInstanceResponse.build();
  }

  public static Message buildPersistStateMessage(String checkpointId) {
    CheckpointManager.InitiateStatefulCheckpoint.Builder builder = CheckpointManager
        .InitiateStatefulCheckpoint
        .newBuilder();

    builder.setCheckpointId(checkpointId);

    return builder.build();
  }

  public static InstanceControlMsg buildRestoreInstanceState(String checkpointId) {
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

  public static InstanceControlMsg buildStartInstanceProcessingMessage(String checkpointId) {
    return InstanceControlMsg.newBuilder()
        .setStartInstanceStatefulProcessing(
            CheckpointManager.StartInstanceStatefulProcessing
                .newBuilder()
                .setCheckpointId(checkpointId)
                .build()
        )
        .build();
  }

  public static InstanceControlMsg buildCheckpointSavedMessage(
      String checkpointId,
      String packingPlanId
  ) {
    CheckpointManager.StatefulConsistentCheckpointSaved.Builder builder = CheckpointManager
        .StatefulConsistentCheckpointSaved
        .newBuilder();

    CheckpointManager.StatefulConsistentCheckpoint.Builder ckptBuilder = CheckpointManager
        .StatefulConsistentCheckpoint
        .newBuilder();

    ckptBuilder.setCheckpointId(checkpointId);
    ckptBuilder.setPackingPlanId(packingPlanId);

    builder.setConsistentCheckpoint(ckptBuilder.build());

    return InstanceControlMsg.newBuilder()
        .setStatefulCheckpointSaved(builder.build())
        .build();
  }

}
