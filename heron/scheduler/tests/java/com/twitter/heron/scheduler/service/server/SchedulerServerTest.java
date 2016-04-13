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
