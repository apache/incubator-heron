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

import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.parameters.DriverMemory;
import org.apache.reef.runtime.common.driver.DriverRuntimeConfigurationOptions.JobControlHandler;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.client.ClientManager;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorHandler;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationHandler;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceManagerStatus;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusHandler;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.types.NamedParameterNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HttpPort;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.VerboseLogMode;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;

public class YarnLauncherTest {
  // This test verifies if launcher correctly provides all heron specific configurations are to reef
  // framework. It does so by ensuring required configs exist. The test fails if an unknown config
  // is found. Presence of unknown config should be verified and then added to the test below
  @Test
  public void getHMDriverConfConstructsReefConfig() throws Exception {
    YarnLauncher launcher = new YarnLauncher();
    YarnLauncher spyLauncher = Mockito.spy(launcher);
    Mockito.doNothing().when(spyLauncher).addLibraryToClasspathSet(Mockito.anyString());

    // inputConf contains configs provided to the launcher
    Map<String, String> inputConf = new HashMap<>();
    // expected contains the reef configs launcher is expected to construct using testConfigs
    Map<String, String> expected = new HashMap<>();

    setConfigs(inputConf, expected, Keys.topologyName(), "topology", TopologyName.class);
    setConfigs(inputConf, expected, Keys.topologyBinaryFile(), "binary", TopologyJar.class);
    setConfigs(inputConf, expected, Keys.topologyPackageFile(), "pack", TopologyPackageName.class);
    setConfigs(inputConf, expected, Keys.cluster(), "cluster", Cluster.class);
    setConfigs(inputConf, expected, Keys.role(), "role", Role.class);
    setConfigs(inputConf, expected, Keys.environ(), "env", Environ.class);
    setConfigs(inputConf, expected, YarnContext.HERON_SCHEDULER_YARN_QUEUE, "q", JobQueue.class);
    setConfigs(inputConf, expected, YarnContext.YARN_SCHEDULER_DRIVER_MEMORY_MB,
        "123", DriverMemory.class);
    setConfigs(inputConf, expected, Keys.corePackageUri(),
        new File(".").getName(), HeronCorePackageName.class);

    // the following expected values are mostly specific to reef runtime
    expected.put(JobControlHandler.class.getSimpleName(), ClientManager.class.getName());
    expected.put(NodeDescriptorHandler.class.getSimpleName(),
        NodeDescriptorHandler.class.getName());
    expected.put(ResourceAllocationHandler.class.getSimpleName(),
        ResourceAllocationHandler.class.getName());
    expected.put(ResourceStatusHandler.class.getSimpleName(),
        ResourceStatusHandler.class.getName());
    expected.put(RuntimeParameters.RuntimeStatusHandler.class.getSimpleName(),
        ResourceManagerStatus.class.getName());
    expected.put(VerboseLogMode.class.getSimpleName(), "false");
    expected.put(HttpPort.class.getSimpleName(), "0");
    expected.put(DriverIdentifier.class.getSimpleName(), "topology");

    Config.Builder builder = new Config.Builder();
    for (String configName : inputConf.keySet()) {
      builder.put(configName, inputConf.get(configName));
    }
    builder.put(Keys.stateManagerClass(), "statemanager");
    builder.put(Keys.packingClass(), "packing");
    Config config = builder.build();

    spyLauncher.initialize(config, null);
    Configuration resultConf = spyLauncher.getHMDriverConf();

    for (NamedParameterNode<?> reefConfigNode : resultConf.getNamedParameters()) {
      String resultValue = resultConf.getNamedParameter(reefConfigNode);
      String expectedValue = expected.get(reefConfigNode.getName());
      if (expectedValue == null) {
        Assert.fail(String.format("Unknown config, %s:%s, provided to heron driver",
            reefConfigNode.getName(), resultConf.getNamedParameter(reefConfigNode)));
      } else {
        Assert.assertEquals(expectedValue, resultValue);
      }
      expected.remove(reefConfigNode.getName());
    }

    Assert.assertEquals(String.format("Missing expected configurations: %s", expected),
        0, expected.size());
  }

  private void setConfigs(Map<String, String> inputConf,
                          Map<String, String> outputConf,
                          String heronConfigKey,
                          String value,
                          Class<?> reefConfigKey) {
    inputConf.put(heronConfigKey, value);
    outputConf.put(reefConfigKey.getSimpleName(), value);
  }
}
