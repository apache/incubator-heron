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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  Map<String, String> testConfigMap = new HashMap<>();
  Map<String, String> expectedMap = new HashMap<>();

  @Test
  public void getHMDriverConfConstructsReefConfig() throws Exception {
    YarnLauncher launcher = new YarnLauncher();
    YarnLauncher spyLauncher = Mockito.spy(launcher);
    Mockito.doNothing().when(spyLauncher).addLibraryToClasspathSet(Mockito.anyString());

    testConfigMap.clear();
    expectedMap.clear();
    setConfigs(Keys.topologyName(), "topology", TopologyName.class);
    setConfigs(Keys.topologyBinaryFile(), "binary", TopologyJar.class);
    setConfigs(Keys.topologyPackageFile(), "package", TopologyPackageName.class);
    setConfigs(Keys.cluster(), "cluster", Cluster.class);
    setConfigs(Keys.role(), "role", Role.class);
    setConfigs(Keys.environ(), "env", Environ.class);
    setConfigs(YarnContext.HERON_SCHEDULER_YARN_QUEUE, "queue", JobQueue.class);
    setConfigs(YarnContext.YARN_SCHEDULER_DRIVER_MEMORY_MB, "123", DriverMemory.class);
    setConfigs(Keys.corePackageUri(), new File(".").getName(), HeronCorePackageName.class);

    Set<String> reefSpecificConfigs = new HashSet<>();
    reefSpecificConfigs.add("JobControlHandler");
    reefSpecificConfigs.add("NodeDescriptorHandler");
    reefSpecificConfigs.add("ResourceAllocationHandler");
    reefSpecificConfigs.add("ResourceStatusHandler");
    reefSpecificConfigs.add("RuntimeStatusHandler");
    reefSpecificConfigs.add("VerboseLogMode");
    reefSpecificConfigs.add("HttpPort");
    reefSpecificConfigs.add("DriverIdentifier");

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
      } else {
        reefSpecificConfigs.remove(reefConfigNode.getName());
      }
    }
    Assert.assertEquals(0, expectedMap.size());
    Assert.assertEquals(0, reefSpecificConfigs.size());
  }

  private void setConfigs(String heronConfigKey, String value, Class<?> reefConfigKey) {
    testConfigMap.put(heronConfigKey, value);
    expectedMap.put(reefConfigKey.getSimpleName(), value);
  }
}
