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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Command;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

@RunWith(PowerMockRunner.class)
public class RuntimeManagerRunnerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private final SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
  private final SpiCommonConfig runtime = Mockito.mock(SpiCommonConfig.class);

  @Before
  public void setUp() throws Exception {
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
  }

  @Test
  public void testCall() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);

    // Restart Runner
    RuntimeManagerRunner restartRunner =
        Mockito.spy(new RuntimeManagerRunner(config, runtime, Command.RESTART, client));
    Mockito.doReturn(true).when(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);
    Assert.assertTrue(restartRunner.call());
    Mockito.verify(restartRunner).restartTopologyHandler(TOPOLOGY_NAME);

    // Kill Runner
    RuntimeManagerRunner killRunner =
        Mockito.spy(new RuntimeManagerRunner(config, runtime, Command.KILL, client));
    Mockito.doReturn(true).when(killRunner).killTopologyHandler(TOPOLOGY_NAME);
    Assert.assertTrue(killRunner.call());
    Mockito.verify(killRunner).killTopologyHandler(TOPOLOGY_NAME);
  }

  @Test
  public void testRestartTopologyHandler() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner =
        new RuntimeManagerRunner(config, runtime, Command.RESTART, client);

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
    RuntimeManagerRunner runner =
        Mockito.spy(new RuntimeManagerRunner(config, runtime, Command.KILL, client));

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
}
