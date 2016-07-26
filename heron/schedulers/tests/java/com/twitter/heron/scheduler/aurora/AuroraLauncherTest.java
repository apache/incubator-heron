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

package com.twitter.heron.scheduler.aurora;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.scheduler.LauncherUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.SchedulerUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SchedulerUtils.class)
public class AuroraLauncherTest {
  @Test
  public void testLaunch() throws Exception {
    AuroraLauncher launcher = Mockito.spy(AuroraLauncher.class);

    // Failed to schedule
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(false).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));

    Assert.assertFalse(launcher.launch(Mockito.mock(PackingPlan.class)));
    PowerMockito.verifyStatic();
    LauncherUtils.onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));

    // Happy path
    PowerMockito.spy(SchedulerUtils.class);
    PowerMockito.doReturn(true).when(SchedulerUtils.class, "onScheduleAsLibrary",
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));

    Assert.assertTrue(launcher.launch(Mockito.mock(PackingPlan.class)));
    PowerMockito.verifyStatic();
    LauncherUtils.onScheduleAsLibrary(
        Mockito.any(Config.class),
        Mockito.any(Config.class),
        Mockito.any(IScheduler.class),
        Mockito.any(PackingPlan.class));

    launcher.close();
  }
}
