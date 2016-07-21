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
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.NullScheduler;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class SchedulerClientFactoryTest {
  private static final String TOPOLOGY_NAME = "shiwei_0924_jiayou";

  @Test
  public void testGetServiceSchedulerClient() throws Exception {
    // Instantiate mock objects
    SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
    SpiCommonConfig runtime = Mockito.mock(SpiCommonConfig.class);
    SchedulerStateManagerAdaptor statemgr = Mockito.mock(SchedulerStateManagerAdaptor.class);

    // Get a ServiceSchedulerClient
    Mockito.when(config.getBooleanValue(ConfigKeys.get("SCHEDULER_IS_SERVICE"), true))
        .thenReturn(true);

    // Mock the runtime object
    Mockito.when(runtime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(statemgr);
    Mockito.when(runtime.getStringValue(Keys.topologyName())).thenReturn(TOPOLOGY_NAME);

    // Failed to getSchedulerLocation
    Mockito.when(statemgr.getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME))).thenReturn(null);
    Assert.assertNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
    Mockito.verify(statemgr).getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME));

    // Get a schedulerLocation successfully
    Mockito.when(statemgr.getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(Scheduler.SchedulerLocation.getDefaultInstance());
    Assert.assertNotNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
  }

  @Test
  public void testGetLibrarySchedulerClient() throws Exception {
    // Instantiate mock objects
    SpiCommonConfig config = Mockito.mock(SpiCommonConfig.class);
    SpiCommonConfig runtime = Mockito.mock(SpiCommonConfig.class);

    // Return a NullScheduler
    Mockito.when(config.getStringValue(ConfigKeys.get("SCHEDULER_CLASS"))).
        thenReturn(NullScheduler.class.getName());

    // Get a LibrarySchedulerClient
    Mockito.when(config.getBooleanValue(ConfigKeys.get("SCHEDULER_IS_SERVICE"))).thenReturn(false);
    Assert.assertNotNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());

    // Return some scheduler class not exist
    final String SCHEDULER_CLASS_NOT_EXIST = "class_not_exist";
    Mockito.when(config.getStringValue(ConfigKeys.get("SCHEDULER_CLASS"))).
        thenReturn(SCHEDULER_CLASS_NOT_EXIST);
    Assert.assertNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
  }
}
