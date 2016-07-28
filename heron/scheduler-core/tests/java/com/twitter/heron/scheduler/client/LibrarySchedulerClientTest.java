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

package com.twitter.heron.scheduler.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.scheduler.IScheduler;

public class LibrarySchedulerClientTest {
  private final SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
  private final SpiCommonConfig runtime = Mockito.mock(SpiCommonConfig.class);

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testRestartTopology() throws Exception {
    IScheduler scheduler = Mockito.mock(IScheduler.class);
    Scheduler.RestartTopologyRequest request =
        Scheduler.RestartTopologyRequest.getDefaultInstance();

    LibrarySchedulerClient client = new LibrarySchedulerClient(config, runtime, scheduler);

    // Failure case
    Mockito.when(scheduler.onRestart(request)).thenReturn(false);

    Assert.assertFalse(client.restartTopology(request));
    Mockito.verify(scheduler).initialize(config, runtime);
    Mockito.verify(scheduler).close();
    Mockito.verify(scheduler).onRestart(request);

    // Success case
    Mockito.when(scheduler.onRestart(request)).thenReturn(true);

    Assert.assertTrue(client.restartTopology(request));
    Mockito.verify(scheduler, Mockito.times(2)).initialize(config, runtime);
    Mockito.verify(scheduler, Mockito.times(2)).close();
    Mockito.verify(scheduler, Mockito.times(2)).onRestart(request);
  }

  @Test
  public void testKillTopology() throws Exception {
    IScheduler scheduler = Mockito.mock(IScheduler.class);
    Scheduler.KillTopologyRequest request = Scheduler.KillTopologyRequest.getDefaultInstance();

    LibrarySchedulerClient client = new LibrarySchedulerClient(config, runtime, scheduler);

    // Failure case
    Mockito.when(scheduler.onKill(request)).thenReturn(false);

    Assert.assertFalse(client.killTopology(request));
    Mockito.verify(scheduler).initialize(config, runtime);
    Mockito.verify(scheduler).close();
    Mockito.verify(scheduler).onKill(request);

    // Success case
    Mockito.when(scheduler.onKill(request)).thenReturn(true);

    Assert.assertTrue(client.killTopology(request));
    Mockito.verify(scheduler, Mockito.times(2)).initialize(config, runtime);
    Mockito.verify(scheduler, Mockito.times(2)).close();
    Mockito.verify(scheduler, Mockito.times(2)).onKill(request);
  }
}
