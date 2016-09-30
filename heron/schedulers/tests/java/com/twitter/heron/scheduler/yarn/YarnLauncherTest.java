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

package com.twitter.heron.scheduler.yarn;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.reef.driver.parameters.DriverMemory;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.types.NamedParameterNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;

public class YarnLauncherTest {
  @Test
  public void getHMDriverConfConstructsReefConfig() throws Exception {
    YarnLauncher launcher = new YarnLauncher();
    YarnLauncher spyLauncher = Mockito.spy(launcher);
    Mockito.doNothing().when(spyLauncher).addLibraryToClasspathSet(Mockito.anyString());

    Map<String, String> testConfigMap = new HashMap<>();
    Map<String, String> expectedMap = new HashMap<>();
    testConfigMap.put(Keys.topologyName(), "topology");
    expectedMap.put(TopologyName.class.getSimpleName(), "topology");
    testConfigMap.put(Keys.topologyBinaryFile(), "binary");
    expectedMap.put(TopologyJar.class.getSimpleName(), "binary");
    testConfigMap.put(Keys.topologyPackageFile(), "package");
    expectedMap.put(TopologyPackageName.class.getSimpleName(), "package");
    testConfigMap.put(Keys.corePackageUri(), new File(".").getAbsolutePath());
    expectedMap.put(HeronCorePackageName.class.getSimpleName(), new File(".").getName());
    testConfigMap.put(Keys.cluster(), "cluster");
    expectedMap.put(Cluster.class.getSimpleName(), "cluster");
    testConfigMap.put(Keys.role(), "role");
    expectedMap.put(Role.class.getSimpleName(), "role");
    testConfigMap.put(Keys.environ(), "env");
    expectedMap.put(Environ.class.getSimpleName(), "env");
    testConfigMap.put(YarnContext.HERON_SCHEDULER_YARN_QUEUE, "queue");
    expectedMap.put(JobQueue.class.getSimpleName(), "queue");
    testConfigMap.put(YarnContext.YARN_SCHEDULER_DRIVER_MEMORY_MB, "100");
    expectedMap.put(DriverMemory.class.getSimpleName(), "100");

    Config.Builder builder = new Config.Builder();
    for (String s : testConfigMap.keySet()) {
      builder.put(s, testConfigMap.get(s));
    }
    builder.put(Keys.stateManagerClass(), "statemanager");
    builder.put(Keys.packingClass(), "packing");
    Config config = builder.build();

    spyLauncher.initialize(config, null);
    Configuration constructedConfig = spyLauncher.getHMDriverConf();

    for (NamedParameterNode<?> reefConfigNode : constructedConfig.getNamedParameters()) {
      String value = expectedMap.get(reefConfigNode.getName());
      if (value != null) {
        Assert.assertEquals(value, constructedConfig.getNamedParameter(reefConfigNode));
        expectedMap.remove(reefConfigNode.getName());
      }
    }
    Assert.assertEquals(0, expectedMap.size());
  }
}
