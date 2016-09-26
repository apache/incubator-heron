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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.Runtime;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class RuntimeManagerRunnerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private final Config config = Mockito.mock(Config.class);
  private final Config runtime = Mockito.mock(Config.class);

  @Before
  public void setUp() throws Exception {
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
  }

  private RuntimeManagerRunner newRuntimeManagerRunner(Command command) {
    return newRuntimeManagerRunner(command, Mockito.mock(ISchedulerClient.class));
  }

  private RuntimeManagerRunner newRuntimeManagerRunner(Command command, ISchedulerClient client) {
    return Mockito.spy(new RuntimeManagerRunner(config, runtime, command, client));
  }

  @Test
  public void testCall() throws Exception {
    // Restart Runner
    RuntimeManagerRunner restartRunner = newRuntimeManagerRunner(Command.RESTART);
    Mockito.doReturn(true).when(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);
    Assert.assertTrue(restartRunner.call());
    Mockito.verify(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);

    // Kill Runner
    RuntimeManagerRunner killRunner = newRuntimeManagerRunner(Command.KILL);
    Mockito.doReturn(true).when(killRunner).killTopologyHandler(TOPOLOGY_NAME);
    Assert.assertTrue(killRunner.call());
    Mockito.verify(killRunner).killTopologyHandler(TOPOLOGY_NAME);
  }

  @Test
  public void testRestartTopologyHandler() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.RESTART, client);

    // Restart container 1, not containing TMaster
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME).setContainerIndex(1).build();
    Mockito.when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(1);

    // Failure case
    Mockito.when(client.restartTopology(restartTopologyRequest)).thenReturn(false);
    Assert.assertFalse(runner.restartTopologyHandler(TOPOLOGY_NAME));
    // Should not invoke DeleteTMasterLocation
    Mockito.verify(adaptor, Mockito.never()).deleteTMasterLocation(TOPOLOGY_NAME);
    // Success case
    Mockito.when(client.restartTopology(restartTopologyRequest)).thenReturn(true);
    Assert.assertTrue(runner.restartTopologyHandler(TOPOLOGY_NAME));
    // Should not invoke DeleteTMasterLocation
    Mockito.verify(adaptor, Mockito.never()).deleteTMasterLocation(TOPOLOGY_NAME);


    // Restart container 0, containing TMaster
    Mockito.when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(0);
    Mockito.when(runtime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(adaptor);
    Mockito.when(adaptor.deleteTMasterLocation(TOPOLOGY_NAME)).thenReturn(false);
    Assert.assertFalse(runner.restartTopologyHandler(TOPOLOGY_NAME));
    // DeleteTMasterLocation should be invoked
    Mockito.verify(adaptor).deleteTMasterLocation(TOPOLOGY_NAME);
  }

  @Test
  public void testKillTopologyHandler() throws Exception {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(TOPOLOGY_NAME).build();
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.KILL, client);

    // Failed to invoke client's killTopology
    Mockito.when(client.killTopology(killTopologyRequest)).thenReturn(false);
    Assert.assertFalse(runner.killTopologyHandler(TOPOLOGY_NAME));
    Mockito.verify(client).killTopology(killTopologyRequest);

    // Failed to clean states
    Mockito.when(client.killTopology(killTopologyRequest)).thenReturn(true);
    Mockito.doReturn(false).when(runner).cleanState(
        Mockito.eq(TOPOLOGY_NAME), Mockito.any(SchedulerStateManagerAdaptor.class));
    Assert.assertFalse(runner.killTopologyHandler(TOPOLOGY_NAME));
    Mockito.verify(client, Mockito.times(2)).killTopology(killTopologyRequest);

    // Success case
    Mockito.doReturn(true).when(runner).cleanState(
        Mockito.eq(TOPOLOGY_NAME), Mockito.any(SchedulerStateManagerAdaptor.class));
    Assert.assertTrue(runner.killTopologyHandler(TOPOLOGY_NAME));
    Mockito.verify(client, Mockito.times(3)).killTopology(killTopologyRequest);
  }

  @PrepareForTest(Runtime.class)
  @Test
  public void testUpdateTopologyHandler() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor manager = mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.UPDATE, client);

    PowerMockito.mockStatic(Runtime.class);
    PowerMockito.when(Runtime.schedulerStateManagerAdaptor(runtime)).thenReturn(manager);

    RoundRobinPacking packing = new RoundRobinPacking();
    String newParallelism = "testSpout:1,testBolt:4";

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

    // Success case
    when(client.updateTopology(updateTopologyRequest)).thenReturn(true);
    Assert.assertTrue(runner.updateTopologyHandler(TOPOLOGY_NAME, newParallelism));
    verify(client, Mockito.times(1)).updateTopology(updateTopologyRequest);
  }

  @Test
  public void testParseNewParallelismParam() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);
    Map<String, Integer> changes = runner.parseNewParallelismParam("foo:1,bar:2");
    Assert.assertEquals(2, changes.size());
    Assert.assertEquals(new Integer(1), changes.get("foo"));
    Assert.assertEquals(new Integer(2), changes.get("bar"));
  }

  @Test
  public void testParseNewParallelismParamEmpty() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);
    Map<String, Integer> changes = runner.parseNewParallelismParam("");
    Assert.assertEquals(0, changes.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNewParallelismParamInvalid1() {
    RuntimeManagerRunner runner = newRuntimeManagerRunner(Command.SUBMIT);

    try {
      runner.parseNewParallelismParam("foo:1,bar2");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(), "Invalid parallelism parameter found. Expected: "
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
      Assert.assertEquals(e.getMessage(), "Invalid parallelism parameter found. Expected: "
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

    Assert.assertEquals(runner.parseNewParallelismParam(delta),
        runner.parallelismDelta(initialCounts, changeRequest));
  }
}
