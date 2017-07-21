// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.kubernetes;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class KubernetesSchedulerTest {
  private static final String[] TOPOLOGY_CONF = {"topology_conf"};
  private static final String TOPOLOGY_NAME = "topology_name";
  private static final int CONTAINER_INDEX = 1;
  private static final String PACKING_PLAN_ID = "packing_plan_id";
  private static final String[] EXECUTOR_CMD = {"executor_cmd"};

  private static KubernetesScheduler scheduler;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    scheduler = Mockito.spy(KubernetesScheduler.class);
    Mockito.doReturn(EXECUTOR_CMD).when(scheduler)
        .getExecutorCommand(Mockito.anyInt());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  @Test
  public void testOnSchedule() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    Mockito.doReturn(TOPOLOGY_CONF)
        .when(scheduler).getTopologyConf(Mockito.any(PackingPlan.class));
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan pplan  =
        new PackingPlan(
            PACKING_PLAN_ID,
            new HashSet<PackingPlan.ContainerPlan>()
        );
    Assert.assertTrue(pplan.getContainers().isEmpty());
    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(pplan));

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan =
        new PackingPlan(PACKING_PLAN_ID, containers);

    // Failed to submit topology due to controller failure
    Mockito.doReturn(false).when(controller).submitTopology(Mockito.any(String[].class));
    Assert.assertFalse(scheduler.onSchedule(validPlan));
    Mockito.verify(controller).submitTopology(Mockito.any(String[].class));

    // Succeed to submit topology
    Mockito.doReturn(true).when(controller).submitTopology(Mockito.any(String[].class));
    Assert.assertTrue(scheduler.onSchedule(validPlan));
    Mockito.verify(controller, Mockito.times(2)).submitTopology(Mockito.any(String[].class));
  }

  @Test
  public void testOnRestart() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Construct RestartTopologyRequest
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME)
            .setContainerIndex(CONTAINER_INDEX)
            .build();

    // Fail to restart
    Mockito.doReturn(false).when(controller).restartApp(Mockito.anyInt());
    Assert.assertFalse(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller).restartApp(Matchers.anyInt());

    // Succeed to restart
    Mockito.doReturn(true).when(controller).restartApp(Mockito.anyInt());
    Assert.assertTrue(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller, Mockito.times(2)).restartApp(Matchers.anyInt());
  }

  @Test
  public void testOnKill() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Fail to kill topology
    Mockito.doReturn(false).when(controller).killTopology();
    Assert.assertFalse(scheduler.onKill(Mockito.any(Scheduler.KillTopologyRequest.class)));
    Mockito.verify(controller).killTopology();

    // Succeed to kill topology
    Mockito.doReturn(true).when(controller).killTopology();
    Assert.assertTrue(scheduler.onKill(Mockito.any(Scheduler.KillTopologyRequest.class)));
    Mockito.verify(controller, Mockito.times(2)).killTopology();
  }

  @Test
  public void testAddContainers() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();

    // Fail to retrieve base pod
    Mockito.doThrow(new IOException()).when(controller).getBasePod(Mockito.anyString());
    exception.expect(TopologyRuntimeManagementException.class);
    scheduler.addContainers(containers);

    // Failure to deploy a container
    Mockito.doReturn(Mockito.anyString()).when(controller).getBasePod(Mockito.anyString());
    Mockito.doThrow(new IOException()).when(controller).deployContainer(Mockito.anyString());
    exception.expect(TopologyRuntimeManagementException.class);
    scheduler.addContainers(containers);

    // Successful deployment
    Mockito.doReturn(Mockito.anyString()).when(controller).getBasePod(Mockito.anyString());
    Mockito.doNothing().when(controller).deployContainer(Mockito.anyString());
    scheduler.addContainers(containers);

  }

  @Test
  public void testRemoveContainers() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(0));

    // Failure to remove container
    Mockito.doThrow(new IOException()).when(controller).removeContainer(Mockito.anyString());
    exception.expect(TopologyRuntimeManagementException.class);
    scheduler.removeContainers(containers);

    // Successful removal
    Mockito.doNothing().when(controller).removeContainer(Mockito.anyString());
    scheduler.removeContainers(containers);
  }

  @Test
  public void testGetJobLinks() throws Exception {
    final String SCHEDULER_URI = "http://k8s.uri";
    final String JOB_LINK = SCHEDULER_URI + KubernetesConstants.JOB_LINK;

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(KubernetesContext.HERON_KUBERNETES_SCHEDULER_URI))
        .thenReturn(SCHEDULER_URI);

    Config mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.getStringValue(Key.TOPOLOGY_NAME))
        .thenReturn(TOPOLOGY_NAME);

    scheduler.initialize(mockConfig, mockRuntime);

    List<String> links = scheduler.getJobLinks();
    Assert.assertEquals(1, links.size());
    System.out.println(links.get(0));
    System.out.println(JOB_LINK);
    Assert.assertTrue(links.get(0).equals(JOB_LINK));
  }
}
