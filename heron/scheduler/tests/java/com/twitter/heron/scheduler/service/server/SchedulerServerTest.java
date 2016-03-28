package com.twitter.heron.scheduler.service.server;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class SchedulerServerTest {
  @Test
  public void testSchedulerServer() throws Exception {
    int freePort = NetworkUtility.getFreePort();
    IScheduler scheduler = Mockito.mock(IScheduler.class);
    LaunchContext context = Mockito.mock(LaunchContext.class);

    SchedulerServer schedulerServer =
        Mockito.spy(new SchedulerServer(scheduler, context, freePort, false));

    Assert.assertEquals(NetworkUtility.getHostName(), schedulerServer.getHost());
    Assert.assertEquals(freePort, schedulerServer.getPort());
  }
}
