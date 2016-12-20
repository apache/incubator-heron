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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class RuntimeManagerRunnerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private final Config config = mock(Config.class);
  private final Config runtime = mock(Config.class);

  @Before
  public void setUp() throws Exception {
    when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
  }

  private RuntimeManagerRunner newRuntimeManagerRunner(Command command) {
    return newRuntimeManagerRunner(command, mock(ISchedulerClient.class));
  }

  private RuntimeManagerRunner newRuntimeManagerRunner(Command command, ISchedulerClient client) {
    return spy(new RuntimeManagerRunner(config, runtime, command, client));
  }

  @Test
  public void testCallRestart() throws Exception {
    // Restart Runner
    RuntimeManagerRunner restartRunner = newRuntimeManagerRunner(Command.RESTART);
    doNothing().when(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);
    restartRunner.call();
    verify(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);
  }

  @Test
  public void testCallKill() throws Exception {
    // Kill Runner
    RuntimeManagerRunner killRunner = newRuntimeManagerRunner(Command.KILL);
    doNothing().when(killRunner).killTopologyHandler(TOPOLOGY_NAME);
    killRunner.call();
    verify(killRunner).killTopologyHandler(TOPOLOGY_NAME);
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testRestartTopologyHandlerFailRestartTopology() {
    ISchedulerClient client = mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.RESTART, client);

    // Restart container 1, not containing TMaster
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME).setContainerIndex(1).build();
    when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(1);
    when(client.restartTopology(restartTopologyRequest)).thenReturn(false);
    try {
      runner.restartTopologyHandler(TOPOLOGY_NAME);
    } finally {
      verify(adaptor, never()).deleteTMasterLocation(TOPOLOGY_NAME);
    }
  }

  @Test
  public void testRestartTopologyHandlerSuccRestartTopology() {
    ISchedulerClient client = mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.RESTART, client);

    // Restart container 1, not containing TMaster
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME).setContainerIndex(1).build();
    when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(1);

    // Success case
    when(client.restartTopology(restartTopologyRequest)).thenReturn(true);
    runner.restartTopologyHandler(TOPOLOGY_NAME);
    // Should not invoke DeleteTMasterLocation
    verify(adaptor, never()).deleteTMasterLocation(TOPOLOGY_NAME);
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testRestartTopologyHandlerFailDeleteTMasterLoc() {
    ISchedulerClient client = mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.RESTART, client);

    // Restart container 1, not containing TMaster
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME).setContainerIndex(1).build();
    when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(1);
    // Restart container 0, containing TMaster
    when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(0);
    when(runtime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(adaptor);
    when(adaptor.deleteTMasterLocation(TOPOLOGY_NAME)).thenReturn(false);
    try {
      runner.restartTopologyHandler(TOPOLOGY_NAME);
    } finally {
      // DeleteTMasterLocation should be invoked
      verify(adaptor).deleteTMasterLocation(TOPOLOGY_NAME);
    }
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testKillTopologyHandlerClientCantKill() {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(TOPOLOGY_NAME).build();
    ISchedulerClient client = mock(ISchedulerClient.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.KILL, client);

    // Failed to invoke client's killTopology
    when(client.killTopology(killTopologyRequest)).thenReturn(false);
    try {
      runner.killTopologyHandler(TOPOLOGY_NAME);
    } finally {
      verify(client).killTopology(killTopologyRequest);
    }
  }

  @Test(expected = TopologyRuntimeManagementException.class)
  public void testKillTopologyHandlerFailCleanState() {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(TOPOLOGY_NAME).build();
    ISchedulerClient client = mock(ISchedulerClient.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.KILL, client);

    // Failed to invoke client's killTopology
    when(client.killTopology(killTopologyRequest)).thenReturn(true);
    doThrow(new TopologyRuntimeManagementException("")).when(runner).cleanState(
        eq(TOPOLOGY_NAME), any(SchedulerStateManagerAdaptor.class));
    try {
      runner.killTopologyHandler(TOPOLOGY_NAME);
    } finally {
      verify(client).killTopology(killTopologyRequest);
    }
  }

  @Test
  public void testKillTopologyHandlerOk() {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(TOPOLOGY_NAME).build();
    ISchedulerClient client = mock(ISchedulerClient.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.KILL, client);

    when(client.killTopology(killTopologyRequest)).thenReturn(true);
    // Success case
    doNothing().when(runner).cleanState(
        eq(TOPOLOGY_NAME), any(SchedulerStateManagerAdaptor.class));
    runner.killTopologyHandler(TOPOLOGY_NAME);
    verify(client).killTopology(killTopologyRequest);
  }

  @PrepareForTest(Runtime.class)
  @Test
  public void testUpdateTopologyHandler() throws Exception {
    String newParallelism = "testSpout:1,testBolt:4";
    doUpdateTopologyHandlerTest(newParallelism, true);
  }

  @PrepareForTest(Runtime.class)
  @Test(expected = TopologyRuntimeManagementException.class)
  public void testUpdateTopologyHandlerWithSameParallelism() throws Exception {
    String newParallelism = "testSpout:2,testBolt:3"; // same as current test packing plan
    doUpdateTopologyHandlerTest(newParallelism, false);
  }

  private void doUpdateTopologyHandlerTest(String newParallelism, boolean expectedResult) {
    ISchedulerClient client = mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor manager = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.UPDATE, client);

    PowerMockito.mockStatic(Runtime.class);
    PowerMockito.when(Runtime.schedulerStateManagerAdaptor(runtime)).thenReturn(manager);

    RoundRobinPacking packing = new RoundRobinPacking();

    PackingPlans.PackingPlan currentPlan =
        PackingTestUtils.testProtoPackingPlan(TOPOLOGY_NAME, packing);
    PackingPlans.PackingPlan proposedPlan =
        PackingTestUtils.testProtoPackingPlan(TOPOLOGY_NAME, packing);
    Map<String, Integer> changeRequests = runner.parseNewParallelismParam(newParallelism);

    when(manager.getPackingPlan(eq(TOPOLOGY_NAME))).thenReturn(currentPlan);
    doReturn(proposedPlan).when(runner).buildNewPackingPlan(
        eq(currentPlan), eq(changeRequests), any(TopologyAPI.Topology.class));

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(currentPlan)
            .setProposedPackingPlan(proposedPlan)
            .build();

    when(client.updateTopology(updateTopologyRequest)).thenReturn(true);
    try {
      runner.updateTopologyHandler(TOPOLOGY_NAME, newParallelism);
    } finally {
      int expectedClientUpdateCalls = expectedResult ? 1 : 0;
      verify(client, times(expectedClientUpdateCalls)).updateTopology(updateTopologyRequest);
    }
  }

  @Test
  public void testParseNewParallelismParam() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);
    Map<String, Integer> changes = runner.parseNewParallelismParam("foo:1,bar:2");
    assertEquals(2, changes.size());
    assertEquals(new Integer(1), changes.get("foo"));
    assertEquals(new Integer(2), changes.get("bar"));
  }

  @Test
  public void testParseNewParallelismParamEmpty() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);
    Map<String, Integer> changes = runner.parseNewParallelismParam("");
    assertEquals(0, changes.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNewParallelismParamInvalid1() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);

    try {
      runner.parseNewParallelismParam("foo:1,bar2");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Invalid parallelism parameter found. Expected: "
          + "<component>:<parallelism>[,<component>:<parallelism>], Found: foo:1,bar2");
      throw e;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNewParallelismParamInvalid12() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);

    try {
      runner.parseNewParallelismParam("foo:1bar:2");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Invalid parallelism parameter found. Expected: "
          + "<component>:<parallelism>[,<component>:<parallelism>], Found: foo:1bar:2");
      throw e;
    }
  }

  @Test
  public void testParallelismDelta() {
    doTestParallelismDelta("foo:1,bar:2", "foo:3", "foo:2");
    doTestParallelismDelta("foo:1,bar:2", "foo:1", "");
    doTestParallelismDelta("foo:1,bar:2", "bar:1", "bar:-1");
  }

  private void doTestParallelismDelta(String initial, String changes, String delta) {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);
    Map<String, Integer> initialCounts = runner.parseNewParallelismParam(initial);
    Map<String, Integer> changeRequest = runner.parseNewParallelismParam(changes);

    assertEquals(runner.parseNewParallelismParam(delta),
        runner.parallelismDelta(initialCounts, changeRequest));
  }
}
