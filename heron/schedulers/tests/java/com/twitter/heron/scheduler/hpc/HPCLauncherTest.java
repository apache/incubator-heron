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

package com.twitter.heron.scheduler.hpc;


import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.utils.SchedulerUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HPCContext.class, SchedulerUtils.class})
public class HPCLauncherTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String WORKING_DIRECTORY = "workingDirectory";

  private static Config createRunnerConfig() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(ConfigKeys.get("TOPOLOGY_NAME"))).thenReturn(TOPOLOGY_NAME);
    Mockito.when(config.getStringValue(ConfigKeys.get("CLUSTER"))).thenReturn(CLUSTER);
    Mockito.when(config.getStringValue(ConfigKeys.get("ROLE"))).thenReturn(ROLE);
    Mockito.when(config.getStringValue(ConfigKeys.get("ENVIRON"))).thenReturn(ENVIRON);

    return config;
  }

  @Test
  public void testLaunch() throws Exception {
    Config config = createRunnerConfig();
    Config runtime = Mockito.mock(Config.class);
    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    PackingPlan plan =
        new PackingPlan(
            "plan.id",
            new HashMap<String, PackingPlan.ContainerPlan>(),
            Mockito.mock(PackingPlan.Resource.class));

    PowerMockito.spy(HPCContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(HPCContext.class, "workingDirectory", config);

    HPCLauncher hpcLauncher = Mockito.spy(new HPCLauncher());
    hpcLauncher.initialize(config, runtime);

    HPCScheduler hpcScheduler = Mockito.spy(new HPCScheduler());
    PowerMockito.doReturn(true).when(hpcScheduler).onSchedule(plan);

    // Failed to download
    Mockito.doReturn(true).when(hpcLauncher).setupWorkingDirectory();
    Assert.assertFalse(hpcLauncher.launch(packingPlan));
    Mockito.verify(hpcLauncher).setupWorkingDirectory();

    // Failed to schedule
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(false).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(PackingPlan.class));
    Assert.assertFalse(hpcLauncher.launch(Mockito.mock(PackingPlan.class)));

    // happy path
    PowerMockito.doReturn(true).when(hpcLauncher).setupWorkingDirectory();
    PowerMockito.mockStatic(SchedulerUtils.class);
    PowerMockito.doReturn(true).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class), Mockito.any(Config.class),
        Mockito.any(Config.class), Mockito.any(PackingPlan.class));
    Mockito.verify(hpcLauncher, Mockito.times(2)).launch(Mockito.any(PackingPlan.class));
    hpcLauncher.close();
  }
}
