/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.slurm;


import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScheduler;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({SlurmContext.class, LauncherUtils.class})
public class SlurmLauncherTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String WORKING_DIRECTORY = "workingDirectory";

  private static Config createRunnerConfig() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    Mockito.when(config.getStringValue(Key.CLUSTER)).thenReturn(CLUSTER);
    Mockito.when(config.getStringValue(Key.ROLE)).thenReturn(ROLE);
    Mockito.when(config.getStringValue(Key.ENVIRON)).thenReturn(ENVIRON);

    return config;
  }

  /**
   * Test slurm scheduler launcher
   */
  @Test
  public void testLaunch() throws Exception {
    Config config = createRunnerConfig();
    Config runtime = Mockito.mock(Config.class);
    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    PackingPlan plan = new PackingPlan("plan.id", new HashSet<PackingPlan.ContainerPlan>());

    PowerMockito.spy(SlurmContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(SlurmContext.class, "workingDirectory", config);

    LauncherUtils mockLauncherUtils = Mockito.mock(LauncherUtils.class);
    PowerMockito.spy(LauncherUtils.class);
    PowerMockito.doReturn(mockLauncherUtils).when(LauncherUtils.class, "getInstance");

    SlurmLauncher slurmLauncher = Mockito.spy(new SlurmLauncher());
    slurmLauncher.initialize(config, runtime);

    SlurmScheduler slurmScheduler = Mockito.spy(new SlurmScheduler());
    PowerMockito.doReturn(true).when(slurmScheduler).onSchedule(plan);

    // Failed to download
    Mockito.doReturn(false).when(slurmLauncher).setupWorkingDirectory();
    Assert.assertFalse(slurmLauncher.launch(packingPlan));
    Mockito.verify(slurmLauncher).setupWorkingDirectory();

    // Failed to schedule
    Mockito.when(mockLauncherUtils.onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class))).thenReturn(false);
    PowerMockito.doReturn(true).when(slurmLauncher).setupWorkingDirectory();
    Assert.assertFalse(slurmLauncher.launch(Mockito.mock(PackingPlan.class)));
    Mockito.verify(mockLauncherUtils).onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));

    // happy path
    Mockito.when(mockLauncherUtils.onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class))).thenReturn(true);
    Assert.assertTrue(slurmLauncher.launch(Mockito.mock(PackingPlan.class)));
    Mockito.verify(slurmLauncher, Mockito.times(3)).launch(Mockito.any(PackingPlan.class));
    Mockito.verify(mockLauncherUtils, Mockito.times(2)).onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));
    slurmLauncher.close();
  }
}
