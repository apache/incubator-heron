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

package org.apache.heron.scheduler.aurora;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.IScheduler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LauncherUtils.class)
public class AuroraLauncherTest {
  @Test
  public void testLaunch() throws Exception {
    Config config = Config.newBuilder().build();
    AuroraLauncher launcher = spy(AuroraLauncher.class);
    launcher.initialize(config, config);

    LauncherUtils mockLauncherUtils = mock(LauncherUtils.class);
    PowerMockito.spy(LauncherUtils.class);
    PowerMockito.doReturn(mockLauncherUtils).when(LauncherUtils.class, "getInstance");

    // Failed to schedule
    when(mockLauncherUtils.onScheduleAsLibrary(
        any(Config.class),
        any(Config.class),
        any(IScheduler.class),
        any(PackingPlan.class))).thenReturn(false);

    assertFalse(launcher.launch(mock(PackingPlan.class)));
    verify(mockLauncherUtils).onScheduleAsLibrary(
        any(Config.class),
        any(Config.class),
        any(IScheduler.class),
        any(PackingPlan.class));

    // Happy path
    when(mockLauncherUtils.onScheduleAsLibrary(
        any(Config.class),
        any(Config.class),
        any(IScheduler.class),
        any(PackingPlan.class))).thenReturn(true);

    assertTrue(launcher.launch(mock(PackingPlan.class)));
    verify(mockLauncherUtils, times(2)).onScheduleAsLibrary(
        any(Config.class),
        any(Config.class),
        any(IScheduler.class),
        any(PackingPlan.class));

    launcher.close();
  }
}
