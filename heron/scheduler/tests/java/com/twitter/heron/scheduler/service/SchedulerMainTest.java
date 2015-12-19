package com.twitter.heron.scheduler.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.api.IConfigLoader;
import com.twitter.heron.scheduler.api.IPackingAlgorithm;
import com.twitter.heron.scheduler.api.IScheduler;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.service.server.SchedulerServer;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.Factory;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.Nullity;
import com.twitter.heron.state.IStateManager;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SchedulerMain.class, Factory.class})
public class SchedulerMainTest {

  private IConfigLoader createConfig() {
    IConfigLoader config = mock(DefaultConfigLoader.class);
    when(config.getUploaderClass()).thenReturn(Nullity.NullUploader.class.getName());
    when(config.getLauncherClass()).thenReturn(Nullity.NullLauncher.class.getName());
    when(config.getSchedulerClass()).thenReturn(Nullity.NullScheduler.class.getName());
    when(config.getPackingAlgorithmClass()).thenReturn(Nullity.EmptyPacking.class.getName());
    when(config.load(anyString(), anyString())).thenReturn(true);
    return config;
  }

  @Test
  public void testSchedulerMainWorkflow() throws Exception {
    SchedulerServer server = mock(SchedulerServer.class);
    IConfigLoader config = createConfig();
    String configLoader = config.getClass().getName();
    String schedulerClass = config.getSchedulerClass();
    String packingAlgorithmClass = config.getPackingAlgorithmClass();
    PowerMockito.spy(SchedulerMain.class);
    PowerMockito.spy(Factory.class);
    PowerMockito.doReturn(config).when(Factory.class, "makeConfigLoader", eq(configLoader));
    PowerMockito.doReturn(mock(IStateManager.class)).when(Factory.class, "makeStateManager", anyString());
    IScheduler scheduler = spy(new Nullity.NullScheduler());
    PowerMockito.doReturn(scheduler).when(Factory.class, "makeScheduler", eq(schedulerClass));
    IPackingAlgorithm packingAlgorithm = spy(new Nullity.EmptyPacking());
    PowerMockito.doReturn(packingAlgorithm).when(Factory.class, "makePackingAlgorithm", eq(packingAlgorithmClass));

    PowerMockito.doReturn(server).when(SchedulerMain.class, "runServer", any(IScheduler.class), any(LaunchContext.class), anyInt());
    PowerMockito.doNothing().when(SchedulerMain.class,
        "setSchedulerLocation",
        any(LaunchContext.class),
        any(SchedulerServer.class));
    SchedulerMain.runScheduler(config.getSchedulerClass(), configLoader, "", NetworkUtility.getFreePort(), "", TopologyAPI.Topology.getDefaultInstance());
    verify(scheduler, times(1)).initialize(any(LaunchContext.class));
    verify(scheduler, atLeastOnce()).schedule(any(PackingPlan.class));
  }
}

