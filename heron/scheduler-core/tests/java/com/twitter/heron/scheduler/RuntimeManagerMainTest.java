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


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.dryrun.UpdateDryRunResponse;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.scheduler.SchedulerException;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.ReflectionUtils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class RuntimeManagerMainTest {
  private static final String TOPOLOGY_NAME = "topologyName";
  private static final String TOPOLOGY_ID = "topologyId";
  private static final Command MOCK_COMMAND = Command.KILL;
  private static final String CLUSTER = "cluster";
  private static final String ROLE = "role";
  private static final String ENVIRON = "env";
  private Config config;

  @Before
  public void setUp() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("CLUSTER"))).thenReturn(CLUSTER);
    when(config.getStringValue(ConfigKeys.get("ROLE"))).thenReturn(ROLE);
    when(config.getStringValue(ConfigKeys.get("ENVIRON"))).thenReturn(ENVIRON);
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testValidateRuntimeManageTopologyNotRunning() throws Exception {
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, MOCK_COMMAND);
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(false);
    runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME);
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testValidateRuntimeManageNoExecState() {
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, MOCK_COMMAND);
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(true);
    when(adaptor.getExecutionState(eq(TOPOLOGY_NAME))).thenReturn(null);
    runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME);
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testValidateRuntimeManageWrongState() {
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, MOCK_COMMAND);
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(true);

    // Topology is running
    ExecutionEnvironment.ExecutionState.Builder stateBuilder =
        ExecutionEnvironment.ExecutionState.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setTopologyId(TOPOLOGY_ID).
            setCluster(CLUSTER).
            setEnviron(ENVIRON);

    final String WRONG_ROLE = "wrong";
    ExecutionEnvironment.ExecutionState wrongState = stateBuilder.setRole(WRONG_ROLE).build();
    // cluster/role/environ not matched
    when(adaptor.getExecutionState(eq(TOPOLOGY_NAME))).thenReturn(wrongState);
    runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME);
  }

  @Test
  public void testValidateRuntimeManageOk() throws TopologyRuntimeManagementException {
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerMain runtimeManagerMain = new RuntimeManagerMain(config, MOCK_COMMAND);
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(true);
    ExecutionEnvironment.ExecutionState.Builder stateBuilder =
        ExecutionEnvironment.ExecutionState.newBuilder().
            setTopologyName(TOPOLOGY_NAME).
            setTopologyId(TOPOLOGY_ID).
            setCluster(CLUSTER).
            setEnviron(ENVIRON);
    // Matched
    ExecutionEnvironment.ExecutionState correctState = stateBuilder.setRole(ROLE).build();
    when(adaptor.getExecutionState(eq(TOPOLOGY_NAME))).thenReturn(correctState);
    runtimeManagerMain.validateRuntimeManage(adaptor, TOPOLOGY_NAME);
  }

  /**
   * Test manageTopology()
   */
  @PrepareForTest(ReflectionUtils.class)
  @Test(expected = TopologyRuntimeManagementException.class)
  public void testManageTopologyNoClass() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, MOCK_COMMAND));

    // Failed to create state manager instance
    final String CLASS_NOT_EXIST = "class_not_exist";
    when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS")))
        .thenReturn(CLASS_NOT_EXIST);
    runtimeManagerMain.manageTopology();
  }

  @PrepareForTest(ReflectionUtils.class)
  @Test(expected = TopologyRuntimeManagementException.class)
  public void testManageTopologyFailValidate() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, MOCK_COMMAND));
    // Valid state manager class
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(IStateManager.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IStateManager.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IStateManager.class.getName()));

    // Failed to valid
    doThrow(new TopologyRuntimeManagementException("")).when(runtimeManagerMain)
        .validateRuntimeManage(any(SchedulerStateManagerAdaptor.class), eq(TOPOLOGY_NAME));
    runtimeManagerMain.manageTopology();
  }

  @PrepareForTest(ReflectionUtils.class)
  @Test(expected = SchedulerException.class)
  public void testManageTopologyFailGetSchdulerClient() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, MOCK_COMMAND));
    // Valid state manager class
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(IStateManager.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IStateManager.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IStateManager.class.getName()));

    // Legal request
    doNothing().when(runtimeManagerMain)
        .validateRuntimeManage(any(SchedulerStateManagerAdaptor.class), eq(TOPOLOGY_NAME));

    // Failed to get ISchedulerClient
    doThrow(new SchedulerException("")).when(runtimeManagerMain).
        getSchedulerClient(any(Config.class));
    runtimeManagerMain.manageTopology();
  }

  @PrepareForTest(ReflectionUtils.class)
  @Test(expected = TopologyRuntimeManagementException.class)
  public void testManageTopologyFailCall() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, MOCK_COMMAND));
    // Valid state manager class
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(IStateManager.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IStateManager.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IStateManager.class.getName()));

    // Legal request
    doNothing().when(runtimeManagerMain)
        .validateRuntimeManage(any(SchedulerStateManagerAdaptor.class), eq(TOPOLOGY_NAME));
    // Successfully get ISchedulerClient
    ISchedulerClient client = mock(ISchedulerClient.class);
    doReturn(client).when(runtimeManagerMain).getSchedulerClient(any(Config.class));

    // Failed to callRuntimeManagerRunner
    doThrow(new TopologyRuntimeManagementException("")).when(runtimeManagerMain)
        .callRuntimeManagerRunner(any(Config.class), eq(client));
    runtimeManagerMain.manageTopology();
  }

  @PrepareForTest({ReflectionUtils.class, Runtime.class})
  @Test(expected = UpdateDryRunResponse.class)
  public void testManageTopologyDryRun() throws Exception {
    config = mock(Config.class);

    // prepare packing class
    ResourceCompliantRRPacking repacking = new ResourceCompliantRRPacking();

    when(config.getStringValue(ConfigKeys.get("REPACKING_CLASS")))
        .thenReturn(IRepacking.class.getName());
    when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS")))
        .thenReturn(IStateManager.class.getName());
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
    when(config.getStringValue(RuntimeManagerRunner.NEW_COMPONENT_PARALLELISM_KEY))
        .thenReturn("testSpout:4,testBolt:5");
    // mock dry-run mode
    when(config.getBooleanValue(ConfigKeys.get("DRY_RUN"), false)).thenReturn(true);
    when(config.getDoubleValue(ConfigKeys.get("INSTANCE_CPU"))).thenReturn(1.0);
    when(config.getByteAmountValue(ConfigKeys.get("INSTANCE_RAM")))
        .thenReturn(ByteAmount.fromGigabytes(1));
    when(config.getByteAmountValue(ConfigKeys.get("INSTANCE_DISK")))
        .thenReturn(ByteAmount.fromGigabytes(1));

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, Command.UPDATE));

    // Mock validate runtime
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(mock(IStateManager.class))
        .when(ReflectionUtils.class, "newInstance", eq(IStateManager.class.getName()));
    PowerMockito.doReturn(repacking)
        .when(ReflectionUtils.class, "newInstance", eq(IRepacking.class.getName()));
    doNothing().when(runtimeManagerMain)
        .validateRuntimeManage(any(SchedulerStateManagerAdaptor.class), eq(TOPOLOGY_NAME));

    // Successfully get ISchedulerClient
    ISchedulerClient client = mock(ISchedulerClient.class);
    doReturn(client).when(runtimeManagerMain).getSchedulerClient(any(Config.class));

    // Mock updateTopologyHandler of runner
    PowerMockito.mockStatic(Runtime.class);
    SchedulerStateManagerAdaptor manager = mock(SchedulerStateManagerAdaptor.class);
    PowerMockito.when(Runtime.schedulerStateManagerAdaptor(any(Config.class))).thenReturn(manager);

    RoundRobinPacking packing = new RoundRobinPacking();
    PackingPlans.PackingPlan currentPlan =
        PackingTestUtils.testProtoPackingPlan(TOPOLOGY_NAME, packing);

    // the actual topology does not matter
    doReturn(TopologyAPI.Topology.getDefaultInstance()).when(manager).
        getTopology(eq(TOPOLOGY_NAME));

    doReturn(currentPlan).when(manager).getPackingPlan(eq(TOPOLOGY_NAME));

    try {
      runtimeManagerMain.manageTopology();
    } finally {
      // verify scheduler client is never used
      verifyZeroInteractions(client);
    }
  }

  @PrepareForTest(ReflectionUtils.class)
  @Test
  public void testManageTopologyOk() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);

    RuntimeManagerMain runtimeManagerMain = spy(new RuntimeManagerMain(config, MOCK_COMMAND));
    // Valid state manager class
    Mockito.when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(IStateManager.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IStateManager.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IStateManager.class.getName()));

    // Legal request
    doNothing().when(runtimeManagerMain)
        .validateRuntimeManage(any(SchedulerStateManagerAdaptor.class), eq(TOPOLOGY_NAME));
    // Successfully get ISchedulerClient
    ISchedulerClient client = mock(ISchedulerClient.class);
    doReturn(client).when(runtimeManagerMain).getSchedulerClient(any(Config.class));
    // Happy path
    doNothing().when(runtimeManagerMain).callRuntimeManagerRunner(any(Config.class), eq(client));
    runtimeManagerMain.manageTopology();
  }
}
