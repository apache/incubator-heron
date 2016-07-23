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

package com.twitter.heron.scheduler.server;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;

public class SchedulerServerTest {
  @Test
  public void testSchedulerServer() throws Exception {
    int freePort = SysUtils.getFreePort();
    IScheduler scheduler = Mockito.mock(IScheduler.class);
    Config runtime = Mockito.mock(Config.class);

    SchedulerServer schedulerServer =
        Mockito.spy(new SchedulerServer(runtime, scheduler, freePort));

    Assert.assertEquals(NetworkUtils.getHostName(), schedulerServer.getHost());
    Assert.assertEquals(freePort, schedulerServer.getPort());
  }
}
