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

package com.twitter.heron.scheduler.ecs;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
//import org.mockito.Matchers;
import org.mockito.Mockito;
//import static org.mockito.Mockito.mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;

import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.TopologyTests;


@RunWith(PowerMockRunner.class)
@PrepareForTest({SchedulerUtils.class, ConfigLoader.class})

public class EcsSchedulerTest {

  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String BUILD_VERSION = "live";
  private static final String BUILD_USER = "user";
  private static final String TOPOLOGY_DEFINITION_FILE = "topologyDefFile";
  private static final String TOPOLOGY_BINARY_FILE = "topologyBinFile";
  private static final String TOPOLOGY_PACKAGE_TYPE = "tar";
  private static final String[] EXECUTOR_CMD = {"executor", "_", "cmd"};
  private static final String DOCKER_CONTENT = "testDockerContent";
  private static final String TEST_DATA_PATH =
      "/__main__/heron/spi/tests/java/com/twitter/heron/spi/common/testdata";
  private final String heronHome =
      Paths.get(System.getenv("JAVA_RUNFILES"), TEST_DATA_PATH).toString();
  private final String configPath = Paths.get(heronHome, "local").toString();
  private static final String TEST_DOCKER_PATH =
      "/__main__/heron/config/src/yaml/conf/ecs/ecs_compose_template.yaml";
  private final String dockerPath = Paths.get(heronHome, "").toString();
  private static EcsScheduler scheduler;
  private Config clusterConfig;
  private Config runtime;
  private EcsContext ecsContext;
  private  Set<PackingPlan.ContainerPlan> containerPlans;

  @Before
  public void before() throws Exception {
    scheduler = Mockito.spy(EcsScheduler.class);

    Config rawConfig = ConfigLoader.loadConfig(
        heronHome, configPath, "/release/file", "/override/file");
    clusterConfig = Mockito.spy(Config.toClusterMode(rawConfig));

    runtime = Mockito.mock(Config.class);

    scheduler.initialize(clusterConfig, runtime);
  }
  @After
  public void after() throws Exception {

  }
  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  private  Config createRunnerRuntime(
      com.twitter.heron.api.Config topologyConfig) throws Exception {
    Config lRuntime = Mockito.spy(Config.newBuilder().build());
    ILauncher launcher = Mockito.mock(ILauncher.class);
    IPacking packing = Mockito.mock(IPacking.class);
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    //TopologyAPI.Topology topology = createTopology(topologyConfig);
    TopologyAPI.Topology topology = TopologyTests.createTopology(
        TOPOLOGY_NAME, new com.twitter.heron.api.Config(), new HashMap<String, Integer>(),
        new HashMap<String, Integer>());

    Mockito.doReturn(launcher).when(lRuntime).get(Key.LAUNCHER_CLASS_INSTANCE);
    Mockito.doReturn(adaptor).when(lRuntime).get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR);
    Mockito.doReturn(topology).when(lRuntime).get(Key.TOPOLOGY_DEFINITION);

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Mockito.when(packingPlan.getContainers()).thenReturn(
        new HashSet<PackingPlan.ContainerPlan>());
    Mockito.when(packingPlan.getComponentRamDistribution()).thenReturn("ramdist");
    Mockito.when(packingPlan.getId()).thenReturn("packing_plan_id");
    containerPlans = new HashSet<>();
    containerPlans.add(PackingTestUtils.testContainerPlan(1)); // just need it to be of size 1
    Mockito.when(packingPlan.getContainers()).thenReturn(containerPlans);
    Mockito.when(packing.pack()).thenReturn(packingPlan);

    LauncherUtils mockLauncherUtils = Mockito.mock(LauncherUtils.class);
    Mockito.when(mockLauncherUtils.createPackingPlan(Mockito.any(Config.class),
        Mockito.any(Config.class)))
        .thenReturn(packingPlan);
    PowerMockito.spy(LauncherUtils.class);
    PowerMockito.doReturn(mockLauncherUtils).when(LauncherUtils.class, "getInstance");


    return lRuntime;
  }
  @Test
  public void testOnSchedule() throws Exception {


    Mockito.doReturn(TOPOLOGY_DEFINITION_FILE).
        when(clusterConfig).get(Key.TOPOLOGY_DEFINITION_FILE);
    Mockito.doReturn(TOPOLOGY_BINARY_FILE).when(clusterConfig).get(Key.TOPOLOGY_BINARY_FILE);
    Mockito.doReturn(PackageType.getPackageType("test.tar")).
        when(clusterConfig).get(Key.TOPOLOGY_PACKAGE_TYPE);

    TopologyAPI.Topology topology = TopologyTests.createTopology(
        TOPOLOGY_NAME, new com.twitter.heron.api.Config(), new HashMap<String, Integer>(),
        new HashMap<String, Integer>());
    Mockito.when(runtime.get(Key.TOPOLOGY_DEFINITION)).thenReturn(topology);
    Mockito.when(runtime.get(Key.TOPOLOGY_DEFINITION_FILE)).thenReturn(TOPOLOGY_DEFINITION_FILE);

    ecsContext = Mockito.mock(EcsContext.class);
    PowerMockito.mockStatic(EcsContext.class);
    Mockito.when(EcsContext.ecsComposeTemplate(clusterConfig)).thenReturn(TEST_DOCKER_PATH);

    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(1));
    //containers.add(PackingTestUtils.testContainerPlan(2));
    //containers.add(PackingTestUtils.testContainerPlan(3));
    Mockito.when(packingPlan.getContainers()).thenReturn(containers);

    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan =
        new PackingPlan(TOPOLOGY_NAME, containers);
    Assert.assertTrue(scheduler.onSchedule(validPlan));
  }
}
