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

package com.twitter.heron.scheduler.slurm;


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
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.SchedulerUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SlurmContext.class, SchedulerUtils.class})
public class SlurmLauncherTest {
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

    PowerMockito.spy(SlurmContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(SlurmContext.class, "workingDirectory", config);

    SlurmLauncher slurmLauncher = Mockito.spy(new SlurmLauncher());
    slurmLauncher.initialize(config, runtime);

    SlurmScheduler slurmScheduler = Mockito.spy(new SlurmScheduler());
    PowerMockito.doReturn(true).when(slurmScheduler).onSchedule(plan);

    // Failed to download
    Mockito.doReturn(false).when(slurmLauncher).setupWorkingDirectory();
    Assert.assertFalse(slurmLauncher.launch(packingPlan));
    Mockito.verify(slurmLauncher).setupWorkingDirectory();

    // Failed to schedule
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(false).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(PackingPlan.class));
    Assert.assertFalse(slurmLauncher.launch(Mockito.mock(PackingPlan.class)));

    // happy path
    PowerMockito.doReturn(true).when(slurmLauncher).setupWorkingDirectory();
    PowerMockito.mockStatic(SchedulerUtils.class);
    PowerMockito.doReturn(true).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class), Mockito.any(Config.class),
        Mockito.any(Config.class), Mockito.any(PackingPlan.class));
    Mockito.verify(slurmLauncher, Mockito.times(2)).launch(Mockito.any(PackingPlan.class));
    slurmLauncher.close();
  }
}
