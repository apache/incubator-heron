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

package com.twitter.heron.resource;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.Ignore;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.config.SystemConfigKey;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.PhysicalPlans;

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
      TopologyAPI.TopologyState topologyState) {
    PhysicalPlans.PhysicalPlan.Builder pPlan = PhysicalPlans.PhysicalPlan.newBuilder();

    setTopology(pPlan, ackEnabled, messageTimeout, topologyState);

    setInstances(pPlan);

    setStMgr(pPlan);

    return pPlan.build();
  }

  public static PhysicalPlans.PhysicalPlan getPhysicalPlan(boolean ackEnabled, int messageTimeout) {
    return getPhysicalPlan(ackEnabled, messageTimeout, TopologyAPI.TopologyState.RUNNING);
  }

  private static void setTopology(PhysicalPlans.PhysicalPlan.Builder pPlan, boolean ackEnabled,
                                  int messageTimeout, TopologyAPI.TopologyState topologyState) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 1);
    // Here we need case switch to corresponding grouping
    topologyBuilder.setBolt("test-bolt", new TestBolt(), 1).shuffleGrouping("test-spout");

    Config conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    if (ackEnabled) {
      conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    } else {
      conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATMOST_ONCE);
    }
    if (messageTimeout != -1) {
      conf.setMessageTimeoutSecs(messageTimeout);
      conf.put("topology.enable.message.timeouts", "true");
    }

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(topologyState).
            getTopology();

    pPlan.setTopology(fTopology);
  }

  private static void setInstances(PhysicalPlans.PhysicalPlan.Builder pPlan) {
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
  }

  private static void setStMgr(PhysicalPlans.PhysicalPlan.Builder pPlan) {
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    pPlan.addStmgrs(stmgr);
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

}
