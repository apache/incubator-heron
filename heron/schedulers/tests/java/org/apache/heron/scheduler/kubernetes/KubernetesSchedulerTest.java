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

package org.apache.heron.scheduler.kubernetes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.TopologyRuntimeManagementException;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.PackingTestUtils;

public class KubernetesSchedulerTest {
  private static final String TOPOLOGY_NAME = "topology-name";
  private static final int CONTAINER_INDEX = 1;
  private static final String PACKING_PLAN_ID = "packing_plan_id";

  private KubernetesScheduler scheduler;

  private Config config;
  private Config mockRuntime;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    scheduler = Mockito.spy(KubernetesScheduler.class);

    config = Config.newBuilder().build();
    mockRuntime = Mockito.mock(Config.class);
    Mockito.when(mockRuntime.getStringValue(Key.TOPOLOGY_NAME))
        .thenReturn(TOPOLOGY_NAME);
  }

  @Test
  public void testOnSchedule() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(config, mockRuntime);

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan pplan  =
        new PackingPlan(
            PACKING_PLAN_ID,
            new HashSet<>()
        );
    Assert.assertTrue(pplan.getContainers().isEmpty());
    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(pplan));

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan =
        new PackingPlan(PACKING_PLAN_ID, containers);

    // Failed to submit topology due to controller failure
    Mockito.doReturn(false).when(controller).submit(Mockito.any(PackingPlan.class));
    Assert.assertFalse(scheduler.onSchedule(validPlan));
    Mockito.verify(controller).submit(Mockito.any(PackingPlan.class));

    // Succeed to submit topology
    Mockito.doReturn(true).when(controller).submit(Mockito.any(PackingPlan.class));
    Assert.assertTrue(scheduler.onSchedule(validPlan));
    Mockito.verify(controller, Mockito.times(2)).submit(Mockito.any(PackingPlan.class));
  }

  @Test
  public void testOnRestart() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(config, mockRuntime);

    // Construct RestartTopologyRequest
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME)
            .setContainerIndex(CONTAINER_INDEX)
            .build();

    // Fail to restart
    Mockito.doReturn(false).when(controller).restart(Mockito.anyInt());
    Assert.assertFalse(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller).restart(Matchers.anyInt());

    // Succeed to restart
    Mockito.doReturn(true).when(controller).restart(Mockito.anyInt());
    Assert.assertTrue(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller, Mockito.times(2)).restart(Matchers.anyInt());
  }

  @Test
  public void testOnKill() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(config, mockRuntime);

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
    scheduler.initialize(config, mockRuntime);
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();

    // Failure to deploy a container
    Mockito.doThrow(new TopologyRuntimeManagementException("")).when(controller)
        .addContainers(Mockito.anySetOf(PackingPlan.ContainerPlan.class));
    expectedException.expect(TopologyRuntimeManagementException.class);
    scheduler.addContainers(containers);

    // Successful deployment
    Mockito.doNothing().when(controller)
        .addContainers(Mockito.anySetOf(PackingPlan.ContainerPlan.class));
    scheduler.addContainers(containers);
  }

  @Test
  public void testRemoveContainers() throws Exception {
    KubernetesController controller = Mockito.mock(KubernetesController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(config, mockRuntime);
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(0));

    // Failure to remove container
    Mockito.doThrow(new TopologyRuntimeManagementException("")).when(controller)
      .removeContainers(Mockito.anySetOf(PackingPlan.ContainerPlan.class));
    expectedException.expect(TopologyRuntimeManagementException.class);
    scheduler.removeContainers(containers);

    // Successful removal
    Mockito.doNothing().when(controller)
        .removeContainers(Mockito.anySetOf(PackingPlan.ContainerPlan.class));
    scheduler.removeContainers(containers);
  }

  @Test
  public void testGetJobLinks() throws Exception {
    final String SCHEDULER_URI = "http://k8s.uri";
    final String JOB_LINK = SCHEDULER_URI + KubernetesConstants.JOB_LINK;

    Config c = Config.newBuilder()
        .put(KubernetesContext.HERON_KUBERNETES_SCHEDULER_URI, SCHEDULER_URI)
        .build();

    scheduler.initialize(c, mockRuntime);

    List<String> links = scheduler.getJobLinks();
    Assert.assertEquals(1, links.size());
    System.out.println(links.get(0));
    System.out.println(JOB_LINK);
    Assert.assertTrue(links.get(0).equals(JOB_LINK));
  }

  @Test
  public void testFetchCommand() throws URISyntaxException {
    final String expectedFetchCommand =
        "/opt/heron/heron-core/bin/heron-downloader https://heron/topology.tar.gz .";

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(Key.DOWNLOADER_BINARY))
        .thenReturn("/opt/heron/heron-core/bin/heron-downloader");

    Config mockRuntimeConfig = Mockito.mock(Config.class);
    Mockito.when(mockRuntimeConfig.get(Key.TOPOLOGY_PACKAGE_URI))
        .thenReturn(new URI("https://heron/topology.tar.gz"));

    Assert.assertEquals(expectedFetchCommand,
        KubernetesUtils.getFetchCommand(mockConfig, mockRuntimeConfig));
  }

  @Test
  public void testValidTopologyName() {
    // test valid names
    Assert.assertTrue(KubernetesScheduler.topologyNameIsValid("topology"));
    Assert.assertTrue(KubernetesScheduler.topologyNameIsValid("test-topology"));
    Assert.assertTrue(KubernetesScheduler.topologyNameIsValid("test.topology"));

    // test invalid names
    final String invalidCharacters = "!@#$%^&*()_+=</|\":\\; ";
    for (int i = 0; i < invalidCharacters.length(); ++i) {
      final String topologyName = "test" + invalidCharacters.charAt(i) + "topology";
      Assert.assertFalse(KubernetesScheduler.topologyNameIsValid(topologyName));
    }
  }

  @Test
  public void testValidImagePullPolicies() {
    // test valid names
    Assert.assertTrue(KubernetesScheduler.imagePullPolicyIsValid(null));
    Assert.assertTrue(KubernetesScheduler.imagePullPolicyIsValid(""));
    KubernetesConstants.VALID_IMAGE_PULL_POLICIES.forEach((String policy) ->
        Assert.assertTrue(KubernetesScheduler.imagePullPolicyIsValid(policy))
    );

    // test invalid names
    Assert.assertFalse(KubernetesScheduler.imagePullPolicyIsValid("never"));
    Assert.assertFalse(KubernetesScheduler.imagePullPolicyIsValid("unknownPolicy"));
  }
}
