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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.SubmitterMain;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.common.TokenSub;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;
import com.twitter.heron.spi.utils.TopologyTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TokenSub.class, Config.class})
public class AuroraSchedulerTest {
  private static final String AURORA_PATH = "path.aurora";
  private static final String PACKING_PLAN_ID = "packing.plan.id";
  private static final String TOPOLOGY_NAME = "topologyName";
  private static final int CONTAINER_ID = 7;

  private static AuroraScheduler scheduler;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    scheduler = Mockito.spy(AuroraScheduler.class);
    doReturn(new HashMap<String, String>())
        .when(scheduler).createAuroraProperties(Mockito.any(Resource.class));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  /**
   * Tests that we can schedule
   */
  @Test
  public void testOnSchedule() throws Exception {
    AuroraController controller = Mockito.mock(AuroraController.class);
    doReturn(controller).when(scheduler).getController();

    SchedulerStateManagerAdaptor stateManager = mock(SchedulerStateManagerAdaptor.class);
    Config runtime = Mockito.mock(Config.class);
    when(runtime.get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR)).thenReturn(stateManager);
    when(runtime.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);

    Config mConfig = Mockito.mock(Config.class);
    PowerMockito.mockStatic(Config.class);
    when(Config.toClusterMode(mConfig)).thenReturn(mConfig);

    when(mConfig.getStringValue(eq(AuroraContext.JOB_TEMPLATE),
        anyString())).thenReturn(AURORA_PATH);

    scheduler.initialize(mConfig, runtime);

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan plan = new PackingPlan(PACKING_PLAN_ID, new HashSet<PackingPlan.ContainerPlan>());
    assertTrue(plan.getContainers().isEmpty());

    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(plan));

    // Construct valid PackingPlan
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(CONTAINER_ID));
    PackingPlan validPlan = new PackingPlan(PACKING_PLAN_ID, containers);

    // Failed to create job via controller
    doReturn(false).when(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    doReturn(true).when(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    Assert.assertFalse(scheduler.onSchedule(validPlan));

    Mockito.verify(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Mockito.verify(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    // Happy path
    doReturn(true).when(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    assertTrue(scheduler.onSchedule(validPlan));

    Mockito.verify(controller, Mockito.times(2))
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Mockito.verify(stateManager, Mockito.times(2))
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));
  }

  @Test
  public void testOnKill() throws Exception {
    Config mockConfig = Mockito.mock(Config.class);
    PowerMockito.mockStatic(Config.class);
    when(Config.toClusterMode(mockConfig)).thenReturn(mockConfig);

    AuroraController controller = Mockito.mock(AuroraController.class);
    doReturn(controller).when(scheduler).getController();
    scheduler.initialize(mockConfig, Mockito.mock(Config.class));

    // Failed to kill job via controller
    doReturn(false).when(controller).killJob();
    Assert.assertFalse(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller).killJob();

    // Happy path
    doReturn(true).when(controller).killJob();
    assertTrue(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller, Mockito.times(2)).killJob();
  }

  @Test
  public void testOnRestart() throws Exception {
    Config mockConfig = Mockito.mock(Config.class);
    PowerMockito.mockStatic(Config.class);
    when(Config.toClusterMode(mockConfig)).thenReturn(mockConfig);

    AuroraController controller = Mockito.mock(AuroraController.class);
    doReturn(controller).when(scheduler).getController();
    scheduler.initialize(mockConfig, Mockito.mock(Config.class));

    // Construct the RestartTopologyRequest
    int containerToRestart = 1;
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(TOPOLOGY_NAME).setContainerIndex(containerToRestart).
            build();

    // Failed to kill job via controller
    doReturn(false).when(
        controller).restart(containerToRestart);
    Assert.assertFalse(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller).restart(containerToRestart);

    // Happy path
    doReturn(true).when(
        controller).restart(containerToRestart);
    assertTrue(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller, Mockito.times(2)).restart(containerToRestart);
  }

  @Test
  public void testGetJobLinks() throws Exception {
    final String JOB_LINK_FORMAT = "http://go/${CLUSTER}/${ROLE}/${ENVIRON}/${TOPOLOGY}";
    final String SUBSTITUTED_JOB_LINK = "http://go/local/heron/test/test_topology";

    Config mockConfig = Mockito.mock(Config.class);
    when(mockConfig.getStringValue(AuroraContext.JOB_LINK_TEMPLATE))
        .thenReturn(JOB_LINK_FORMAT);

    PowerMockito.mockStatic(Config.class);
    when(Config.toClusterMode(mockConfig)).thenReturn(mockConfig);

    scheduler.initialize(mockConfig, Mockito.mock(Config.class));

    PowerMockito.spy(TokenSub.class);
    PowerMockito.doReturn(SUBSTITUTED_JOB_LINK)
        .when(TokenSub.class, "substitute", mockConfig, JOB_LINK_FORMAT);

    List<String> result = scheduler.getJobLinks();

    assertEquals(1, result.size());
    assertTrue(result.get(0).equals(SUBSTITUTED_JOB_LINK));
  }


  @Test
  public void testProperties() throws URISyntaxException {
    TopologyAPI.Topology topology = TopologyTests.createTopology(
        TOPOLOGY_NAME, new com.twitter.heron.api.Config(),
        "spoutName", "boltName", 1, 1);

    Config runtime = mock(Config.class);
    when(runtime.get(Key.TOPOLOGY_DEFINITION)).thenReturn(topology);
    when(runtime.get(Key.TOPOLOGY_PACKAGE_URI)).thenReturn(new URI("http://foo/bar"));

    // This must mimic how SubmitterMain loads configs
    CommandLine commandLine = mock(CommandLine.class);
    when(commandLine.getOptionValue("cluster")).thenReturn("some_cluster");
    when(commandLine.getOptionValue("role")).thenReturn("some_role");
    when(commandLine.getOptionValue("environment")).thenReturn("some_env");
    when(commandLine.getOptionValue("heron_home")).thenReturn("/some/heron/home");
    when(commandLine.getOptionValue("config_path")).thenReturn("/some/config/path");
    when(commandLine.getOptionValue("topology_package")).thenReturn("jar");
    when(commandLine.getOptionValue("topology_defn")).thenReturn("/mock/defnFile.defn");
    when(commandLine.getOptionValue("topology_bin")).thenReturn("binaryFile.jar");
    Config config = Mockito.spy(SubmitterMain.loadConfig(commandLine, topology));

    AuroraScheduler testScheduler = new AuroraScheduler();
    testScheduler.initialize(config, runtime);
    Resource containerResource =
        new Resource(2.3, ByteAmount.fromGigabytes(2), ByteAmount.fromGigabytes(3));
    Map<AuroraField, String> properties = testScheduler.createAuroraProperties(containerResource);

    // this part is key, the conf path in the config is absolute to the install dir, but what
    // aurora properties get below is the relative ./heron-conf path to be used when run remotely
    assertEquals("Invalid value for key " + Key.HERON_CONF,
        "/some/config/path", config.getStringValue(Key.HERON_CONF));

    String expectedConf = "./heron-conf";
    String expectedBin = "./heron-core/bin";
    String expectedLib = "./heron-core/lib";
    String expectedDist = "./heron-core/dist";
    for (AuroraField field : AuroraField.values()) {
      boolean asserted = false;
      Object expected = null;
      Object found = properties.get(field);
      switch (field) {
        case CLUSTER:
          expected = "some_cluster";
          break;
        case ENVIRON:
          expected = "some_env";
          break;
        case ROLE:
          expected = "some_role";
          break;
        case COMPONENT_RAMMAP:
        case STATEMGR_CONNECTION_STRING:
        case STATEMGR_ROOT_PATH:
          expected = null;
          break;
        case COMPONENT_JVM_OPTS_IN_BASE64:
        case INSTANCE_JVM_OPTS_IN_BASE64:
          expected = "\"\"";
          break;
        case CORE_PACKAGE_URI:
          expected = expectedDist + "/heron-core.tar.gz";
          break;
        case CPUS_PER_CONTAINER:
          expected = Double.valueOf(containerResource.getCpu()).toString();
          break;
        case DISK_PER_CONTAINER:
          expected = Long.valueOf(containerResource.getDisk().asBytes()).toString();
          break;
        case RAM_PER_CONTAINER:
          expected = Long.valueOf(containerResource.getRam().asBytes()).toString();
          break;
        case JAVA_HOME:
          expected = "/usr/lib/jvm/default-java";
          break;
        case TIER:
          expected = "preemptible";
          break;
        case NUM_CONTAINERS:
          expected = "2";
          break;
        case EXECUTOR_BINARY:
          expected = expectedBin + "/heron-executor";
          break;
        case INSTANCE_CLASSPATH:
          expected = expectedLib + "/instance/*";
          break;
        case METRICSMGR_CLASSPATH:
          expected = expectedLib + "/metricsmgr/*";
          break;
        case METRICS_YAML:
          expected = expectedConf + "/metrics_sinks.yaml";
          break;
        case PYTHON_INSTANCE_BINARY:
          expected = expectedBin + "/heron-python-instance";
          break;
        case SCHEDULER_CLASSPATH:
          expected =
              expectedLib + "/scheduler/*:./heron-core/lib/packing/*:./heron-core/lib/statemgr/*";
          break;
        case SHELL_BINARY:
          expected = expectedBin + "/heron-shell";
          break;
        case STMGR_BINARY:
          expected = expectedBin + "/heron-stmgr";
          break;
        case TMASTER_BINARY:
          expected = expectedBin + "/heron-tmaster";
          break;
        case SYSTEM_YAML:
          expected = expectedConf + "/heron_internals.yaml";
          break;
        case TOPOLOGY_BINARY_FILE:
        case TOPOLOGY_CLASSPATH:
          expected = "binaryFile.jar";
          break;
        case TOPOLOGY_DEFINITION_FILE:
          expected = "defnFile.defn";
          break;
        case TOPOLOGY_ID:
          assertTrue(field + " does not start with topologyName: " + found,
              found.toString().startsWith("topologyName"));
          asserted = true;
          break;
        case TOPOLOGY_NAME:
          expected = "topologyName";
          break;
        case TOPOLOGY_PACKAGE_TYPE:
          expected = "jar";
          break;
        case TOPOLOGY_PACKAGE_URI:
          expected = "http://foo/bar";
          break;
        case METRICSCACHEMGR_CLASSPATH:
          expected = expectedLib + "/metricscachemgr/*";
          break;
        case CKPTMGR_CLASSPATH:
          expected =
              expectedLib + "/ckptmgr/*:" + expectedLib + "/statefulstorage/*:";
          break;
        case IS_STATEFUL_ENABLED:
          expected = Boolean.FALSE.toString();
          break;
        case STATEFUL_CONFIG_YAML:
          expected = expectedConf + "/stateful.yaml";
          break;
        default:
          fail(String.format(
              "Expected value for Aurora field %s not found in test (found=%s)", field, found));
      }
      if (!asserted) {
        assertEquals("Incorrect value found for field " + field, expected, found);
      }
      properties.remove(field);
    }

    assertTrue("The following aurora fields were not set by the scheduler: " + properties,
        properties.isEmpty());
  }
}
