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

package com.twitter.heron.scheduler.aurora;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.RuntimeManagerContext;

public class AuroraTopologyRuntimeManagerTest {
  private static final String cluster = "cluster";
  private static final String environ = "environ";
  private static final String role = "role";
  private static final String tmasterHost = "tmaster.host";
  private static final int tmasterControlPort = 123;
  private static final String topologyName = "topology";
  private static final String stateMgrClass = "com.twitter.heron.statemgr.NullStateManager";

  AuroraConfigLoader createRequiredConfig() throws Exception {
    AuroraConfigLoader schedulerConfig = AuroraConfigLoader.class.newInstance();
    schedulerConfig.addDefaultProperties();
    schedulerConfig.properties.setProperty(Constants.CLUSTER, cluster);
    schedulerConfig.properties.setProperty(Constants.ROLE, role);
    schedulerConfig.properties.setProperty(Constants.ENVIRON, environ);
    schedulerConfig.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_ROLE, "me");
    schedulerConfig.properties.getProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "some-pkg");
    schedulerConfig.properties.getProperty(Constants.HERON_RELEASE_PACKAGE_VERSION, "live");
    schedulerConfig.properties.setProperty(Constants.HERON_UPLOADER_VERSION, "1");
    schedulerConfig.properties.setProperty(Constants.STATE_MANAGER_CLASS, stateMgrClass);
    return schedulerConfig;
  }

  private ExecutionEnvironment.ExecutionState dummyExecutionState() {
    return ExecutionEnvironment.ExecutionState.newBuilder()
        .setRole(role)
        .setCluster(cluster)
        .setEnviron(environ)
        .setTopologyId(topologyName)
        .setTopologyName(topologyName)
        .build();
  }

  private TopologyMaster.TMasterLocation dummyTMasterLocation() {
    return TopologyMaster.TMasterLocation.newBuilder()
        .setHost(tmasterHost)
        .setMasterPort(0)
        .setTopologyId(topologyName)
        .setTopologyName(topologyName)
        .setControllerPort(tmasterControlPort)
        .build();
  }

  private TopologyAPI.Topology dummyTopology(TopologyAPI.TopologyState state) {
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.setNumStmgrs(2);
    Map<String, Integer> spouts = new HashMap<>();
    int componentParallelism = 2;
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    return TopologyAPI.Topology.newBuilder().mergeFrom(
        TopologyUtilityTest.createTopology(topologyName, topologyConfig, spouts, bolts))
        .setState(state)
        .build();
  }

  @Test
  public void testVerifyState() throws Exception {
    AuroraConfigLoader config = createRequiredConfig();
    AuroraTopologyRuntimeManager runtimeManager = AuroraTopologyRuntimeManager.class.newInstance();
    RuntimeManagerContext context =
        new RuntimeManagerContext(config, topologyName);
    runtimeManager.initialize(context);
    Assert.assertTrue(runtimeManager.verifyState(
        false, dummyExecutionState(), dummyTopology(TopologyAPI.TopologyState.RUNNING),
        dummyTMasterLocation()));
    Assert.assertTrue(runtimeManager.verifyState(
        true, dummyExecutionState(), dummyTopology(TopologyAPI.TopologyState.PAUSED),
        dummyTMasterLocation()));
    // Don't allow topology activation for incorrect role.
    config.properties.setProperty(Constants.ROLE, "not-correct-role");
    runtimeManager.initialize(context);
    Assert.assertFalse(runtimeManager.verifyState(
        true, dummyExecutionState(), dummyTopology(TopologyAPI.TopologyState.PAUSED),
        dummyTMasterLocation()));
  }
}
