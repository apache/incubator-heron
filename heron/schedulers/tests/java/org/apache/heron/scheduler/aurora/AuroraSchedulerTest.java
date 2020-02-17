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

package org.apache.heron.scheduler.aurora;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.commons.cli.CommandLine;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.utils.topology.TopologyTests;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.scheduler.SubmitterMain;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.common.TokenSub;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.PackingTestUtils;

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
@PowerMockIgnore("jdk.internal.reflect.*")
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
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class),
            Matchers.anyMapOf(String.class, String.class));
    doReturn(true).when(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    Assert.assertFalse(scheduler.onSchedule(validPlan));

    Mockito.verify(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class),
            Matchers.anyMapOf(String.class, String.class));
    Mockito.verify(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    // Happy path
    doReturn(true).when(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class),
            Matchers.anyMapOf(String.class, String.class));
    assertTrue(scheduler.onSchedule(validPlan));

    Mockito.verify(controller, Mockito.times(2))
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class),
            Matchers.anyMapOf(String.class, String.class));
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

    AuroraController controller = Mockito.mock(AuroraController.class);
    doReturn(controller).when(scheduler).getController();
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
        TOPOLOGY_NAME, new org.apache.heron.api.Config(),
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
        case TIER:
          expected = "preemptible";
          break;
        case NUM_CONTAINERS:
          expected = "2";
          break;
        case EXECUTOR_BINARY:
          expected = expectedBin + "/heron-executor";
          break;
        case TOPOLOGY_PACKAGE_URI:
          expected = "http://foo/bar";
          break;
        case TOPOLOGY_ARGUMENTS:
          expected = "--topology-name=topologyName"
            + " --topology-id=" + topology.getId()
            + " --topology-defn-file=defnFile.defn"
            + " --state-manager-connection=null"
            + " --state-manager-root=null"
            + " --state-manager-config-file=" + expectedConf + "/statemgr.yaml"
            + " --tmaster-binary=" + expectedBin + "/heron-tmaster"
            + " --stmgr-binary=" + expectedBin + "/heron-stmgr"
            + " --metrics-manager-classpath=" + expectedLib + "/metricsmgr/*"
            + " --instance-jvm-opts=\"\""
            + " --classpath=binaryFile.jar"
            + " --heron-internals-config-file=" + expectedConf + "/heron_internals.yaml"
            + " --override-config-file=" + expectedConf + "/override.yaml"
            + " --component-ram-map=null"
            + " --component-jvm-opts=\"\""
            + " --pkg-type=jar"
            + " --topology-binary-file=binaryFile.jar"
            + " --heron-java-home=/usr/lib/jvm/default-java"
            + " --heron-shell-binary=" + expectedBin + "/heron-shell"
            + " --cluster=some_cluster"
            + " --role=some_role"
            + " --environment=some_env"
            + " --instance-classpath=" + expectedLib + "/instance/*"
            + " --metrics-sinks-config-file=" + expectedConf + "/metrics_sinks.yaml"
            + " --scheduler-classpath=" + expectedLib + "/scheduler/*:./heron-core"
            + "/lib/packing/*:" + expectedLib + "/statemgr/*"
            + " --python-instance-binary=" + expectedBin + "/heron-python-instance"
            + " --cpp-instance-binary=" + expectedBin + "/heron-cpp-instance"
            + " --metricscache-manager-classpath=" + expectedLib + "/metricscachemgr/*"
            + " --metricscache-manager-mode=disabled"
            + " --is-stateful=false"
            + " --checkpoint-manager-classpath=" + expectedLib + "/ckptmgr/*:"
            + expectedLib + "/statefulstorage/*:"
            + " --stateful-config-file=" + expectedConf + "/stateful.yaml"
            + " --checkpoint-manager-ram=1073741824"
            + " --health-manager-mode=disabled"
            + " --health-manager-classpath=" + expectedLib + "/healthmgr/*";
          break;
        case CLUSTER:
          expected = "some_cluster";
          break;
        case ENVIRON:
          expected = "some_env";
          break;
        case ROLE:
          expected = "some_role";
          break;
        case TOPOLOGY_NAME:
          expected = "topologyName";
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
