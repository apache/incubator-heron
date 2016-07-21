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

package com.twitter.heron.scheduler;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.statemgr.NullStateManager;

public class RuntimeManagerMainTest {
  private static final String TOPOLOGY_NAME = "topologyName";
  private static final String TOPOLOGY_ID = "topologyId";
  private static final Command MOCK_COMMAND = Command.KILL;

  @Test
  public void testValidateRuntimeManage() throws Exception {
    final String CLUSTER = "cluster";
    final String ROLE = "role";
    final String ENVIRON = "env";
    SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
    Mockito.when(config.getStringValue(ConfigKeys.get("CLUSTER"))).thenReturn(CLUSTER);
    Mockito.when(config.getStringValue(ConfigKeys.get("ROLE"))).thenReturn(ROLE);
    Mockito.when(config.getStringValue(ConfigKeys.get("ENVIRON"))).thenReturn(ENVIRON);

    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, MOCK_COMMAND);

    // Topology is not running
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(false);
    Assert.assertFalse(runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME));
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(null);
    Assert.assertFalse(runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME));

    // Topology is running
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(true);
    ExecutionEnvironment.ExecutionState.Builder stateBuilder =
        ExecutionEnvironment.ExecutionState.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setTopologyId(TOPOLOGY_ID).
            setCluster(CLUSTER).
            setEnviron(ENVIRON);

    // cluster/role/environ not matched
    final String WRONG_ROLE = "wrong";
    ExecutionEnvironment.ExecutionState wrongState = stateBuilder.setRole(WRONG_ROLE).build();
    Mockito.when(adaptor.getExecutionState(Mockito.eq(TOPOLOGY_NAME))).thenReturn(null);
    Assert.assertFalse(runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME));
    Mockito.when(adaptor.getExecutionState(Mockito.eq(TOPOLOGY_NAME))).thenReturn(wrongState);
    Assert.assertFalse(runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME));

    // Matched
    ExecutionEnvironment.ExecutionState correctState = stateBuilder.setRole(ROLE).build();
    Mockito.when(adaptor.getExecutionState(Mockito.eq(TOPOLOGY_NAME))).thenReturn(correctState);
    Assert.assertTrue(runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME));
  }

  /**
   * Test manageTopology()
   */
  @Test
  public void testManageTopology() throws Exception {
    SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain =
        Mockito.spy(new RuntimeManagerMain(config, MOCK_COMMAND));

    // Failed to create state manager instance
    final String CLASS_NOT_EXIST = "class_not_exist";
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(CLASS_NOT_EXIST);
    Assert.assertFalse(runtimeManagerMain.manageTopology());

    // Valid state manager class
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(NullStateManager.class.getName());

    // Failed to valid
    Mockito.doReturn(false).when(runtimeManagerMain).
        validateRuntimeManage(Mockito.any(SchedulerStateManagerAdaptor.class),
            Mockito.eq(TOPOLOGY_NAME));
    Assert.assertFalse(runtimeManagerMain.manageTopology());

    // Legal request
    Mockito.doReturn(true).when(runtimeManagerMain).
        validateRuntimeManage(Mockito.any(SchedulerStateManagerAdaptor.class),
            Mockito.eq(TOPOLOGY_NAME));

    // Failed to get ISchedulerClient
    Mockito.doReturn(null).when(runtimeManagerMain).
        getSchedulerClient(Mockito.any(SpiCommonConfig.class));
    Assert.assertFalse(runtimeManagerMain.manageTopology());

    // Successfully get ISchedulerClient
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    Mockito.doReturn(client).when(runtimeManagerMain).
        getSchedulerClient(Mockito.any(SpiCommonConfig.class));

    // Failed to callRuntimeManagerRunner
    Mockito.doReturn(false).when(runtimeManagerMain).
        callRuntimeManagerRunner(Mockito.any(SpiCommonConfig.class), Mockito.eq(client));
    Assert.assertFalse(runtimeManagerMain.manageTopology());

    // Happy path
    Mockito.doReturn(true).when(runtimeManagerMain).
        callRuntimeManagerRunner(Mockito.any(SpiCommonConfig.class), Mockito.eq(client));
    Assert.assertTrue(runtimeManagerMain.manageTopology());
  }
}
