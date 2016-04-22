package com.twitter.heron.scheduler;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.scheduler.Command;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.SchedulerUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SchedulerUtils.class})
public class RuntimeManagerRunnerTest {
  private static final String topologyName = "testTopology";
  private final Config config = Mockito.mock(Config.class);
  private final Config runtime = Mockito.mock(Config.class);

  @Before
  public void setUp() throws Exception {
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(topologyName);
  }

  @Test
  public void testCall() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);

    // Restart Runner
    RuntimeManagerRunner restartRunner =
        Mockito.spy(new RuntimeManagerRunner(config, runtime, Command.RESTART, client));
    Mockito.doReturn(true).when(restartRunner).restartTopologyHandler(topologyName);
    Assert.assertTrue(restartRunner.call());
    Mockito.verify(restartRunner).restartTopologyHandler(topologyName);

    // Kill Runner
    RuntimeManagerRunner killRunner =
        Mockito.spy(new RuntimeManagerRunner(config, runtime, Command.KILL, client));
    Mockito.doReturn(true).when(killRunner).killTopologyHandler(topologyName);
    Assert.assertTrue(killRunner.call());
    Mockito.verify(killRunner).killTopologyHandler(topologyName);
  }

  @Test
  public void testRestartTopologyHandler() throws Exception {
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    RuntimeManagerRunner runner = new RuntimeManagerRunner(config, runtime, Command.RESTART, client);

    // Restart container 1, not containing TMaster
    Scheduler.RestartTopologyRequest restartTopologyRequest = Scheduler.RestartTopologyRequest.newBuilder()
        .setTopologyName(topologyName).setContainerIndex(1).build();
    Mockito.when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(1);

    // Failure case
    Mockito.when(client.restartTopology(restartTopologyRequest)).thenReturn(false);
    Assert.assertFalse(runner.restartTopologyHandler(topologyName));
    // Should not invoke DeleteTMasterLocation
    Mockito.verify(adaptor, Mockito.never()).deleteTMasterLocation(topologyName);
    // Success case
    Mockito.when(client.restartTopology(restartTopologyRequest)).thenReturn(true);
    Assert.assertTrue(runner.restartTopologyHandler(topologyName));
    // Should not invoke DeleteTMasterLocation
    Mockito.verify(adaptor, Mockito.never()).deleteTMasterLocation(topologyName);


    // Restart container 0, containing TMaster
    Mockito.when(config.getIntegerValue(ConfigKeys.get("TOPOLOGY_CONTAINER_ID"))).thenReturn(0);
    Mockito.when(runtime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(adaptor);
    Mockito.when(adaptor.deleteTMasterLocation(topologyName)).thenReturn(false);
    Assert.assertFalse(runner.restartTopologyHandler(topologyName));
    // DeleteTMasterLocation should be invoked
    Mockito.verify(adaptor).deleteTMasterLocation(topologyName);
  }

  @Test
  public void testKillTopologyHandler() throws Exception {
    Scheduler.KillTopologyRequest killTopologyRequest = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build();
    ISchedulerClient client = Mockito.mock(ISchedulerClient.class);
    RuntimeManagerRunner runner = new RuntimeManagerRunner(config, runtime, Command.KILL, client);

    // Failed to invoke client's killTopology
    Mockito.when(client.killTopology(killTopologyRequest)).thenReturn(false);
    Assert.assertFalse(runner.killTopologyHandler(topologyName));
    Mockito.verify(client).killTopology(killTopologyRequest);

    // Failed to clean states
    Mockito.when(client.killTopology(killTopologyRequest)).thenReturn(true);
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(false).when(SchedulerUtils.class, "cleanState", Mockito.eq(topologyName),
        Mockito.any(SchedulerStateManagerAdaptor.class));
    Assert.assertFalse(runner.killTopologyHandler(topologyName));
    Mockito.verify(client, Mockito.times(2)).killTopology(killTopologyRequest);

    // Success case
    PowerMockito.doReturn(true).when(SchedulerUtils.class, "cleanState", Mockito.eq(topologyName),
        Mockito.any(SchedulerStateManagerAdaptor.class));
    Assert.assertTrue(runner.killTopologyHandler(topologyName));
    Mockito.verify(client, Mockito.times(3)).killTopology(killTopologyRequest);
  }
}
