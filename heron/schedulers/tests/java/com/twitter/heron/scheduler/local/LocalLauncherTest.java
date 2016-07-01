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

package com.twitter.heron.scheduler.local;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LocalContext.class)
public class LocalLauncherTest {
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
    URI mockURI = new URI("h:a");
    Mockito.doReturn(mockURI).when(runtime).get(Keys.topologyPackageUri());
    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);

    PowerMockito.spy(LocalContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(LocalContext.class, "workingDirectory", config);

    LocalLauncher localLauncher = Mockito.spy(new LocalLauncher());
    localLauncher.initialize(config, runtime);

    // Failed to setup working directory
    Mockito.doReturn(false).when(localLauncher).setupWorkingDirectory();
    Assert.assertFalse(localLauncher.launch(packingPlan));

    // setup successfully
    Mockito.doReturn(true).when(localLauncher).setupWorkingDirectory();
    String[] expectedSchedulerCommand = {"expected", "scheduler", "command"};
    Mockito.doReturn(expectedSchedulerCommand).when(localLauncher).getSchedulerCommand();

    // Failed to start the Scheduler Process
    Mockito.doReturn(null).when(localLauncher).startScheduler(Mockito.any(String[].class));
    Assert.assertFalse(localLauncher.launch(packingPlan));
    Mockito.verify(localLauncher).startScheduler(expectedSchedulerCommand);

    // Happy path
    Mockito.doReturn(Mockito.mock(Process.class)).
        when(localLauncher).startScheduler(Mockito.any(String[].class));
    Assert.assertTrue(localLauncher.launch(packingPlan));
    Mockito.verify(localLauncher, Mockito.times(2)).startScheduler(expectedSchedulerCommand);

    localLauncher.close();
  }
}
