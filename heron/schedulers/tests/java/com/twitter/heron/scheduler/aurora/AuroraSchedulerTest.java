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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Misc;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.PackingTestUtils;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Misc.class)
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
    Mockito.doReturn(new HashMap<String, String>()).when(
        scheduler).createAuroraProperties(
        Mockito.any(PackingPlan.class));
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
    Mockito.doReturn(controller).when(scheduler).getController();

    SchedulerStateManagerAdaptor stateManager = mock(SchedulerStateManagerAdaptor.class);
    Config runtime = Mockito.mock(Config.class);
    Mockito.when(runtime.get(Keys.schedulerStateManagerAdaptor())).thenReturn(stateManager);
    Mockito.when(runtime.getStringValue(Keys.topologyName())).thenReturn(TOPOLOGY_NAME);

    Config mConfig = Mockito.mock(Config.class);
    Mockito.when(mConfig.getStringValue(eq(AuroraContext.JOB_TEMPLATE),
        anyString())).thenReturn(AURORA_PATH);

    scheduler.initialize(mConfig, runtime);

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan plan = new PackingPlan(PACKING_PLAN_ID, new HashSet<PackingPlan.ContainerPlan>());
    Assert.assertTrue(plan.getContainers().isEmpty());

    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(plan));

    // Construct valid PackingPlan
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(CONTAINER_ID));
    PackingPlan validPlan = new PackingPlan(PACKING_PLAN_ID, containers);

    // Failed to create job via controller
    Mockito.doReturn(false).when(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Mockito.doReturn(true).when(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    Assert.assertFalse(scheduler.onSchedule(validPlan));

    Mockito.verify(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Mockito.verify(stateManager)
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));

    // Happy path
    Mockito.doReturn(true).when(controller)
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Assert.assertTrue(scheduler.onSchedule(validPlan));

    Mockito.verify(controller, Mockito.times(2))
        .createJob(Matchers.anyMapOf(AuroraField.class, String.class));
    Mockito.verify(stateManager, Mockito.times(2))
        .updatePackingPlan(any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));
  }

  @Test
  public void testOnKill() throws Exception {
    AuroraController controller = Mockito.mock(AuroraController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Failed to kill job via controller
    Mockito.doReturn(false).when(controller).killJob();
    Assert.assertFalse(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller).killJob();

    // Happy path
    Mockito.doReturn(true).when(controller).killJob();
    Assert.assertTrue(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller, Mockito.times(2)).killJob();
  }

  @Test
  public void testOnRestart() throws Exception {
    AuroraController controller = Mockito.mock(AuroraController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Construct the RestartTopologyRequest
    int containerToRestart = 1;
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(TOPOLOGY_NAME).setContainerIndex(containerToRestart).
            build();

    // Failed to kill job via controller
    Mockito.doReturn(false).when(
        controller).restart(containerToRestart);
    Assert.assertFalse(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller).restart(containerToRestart);

    // Happy path
    Mockito.doReturn(true).when(
        controller).restart(containerToRestart);
    Assert.assertTrue(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller, Mockito.times(2)).restart(containerToRestart);
  }

  @Test
  public void testGetJobLinks() throws Exception {
    final String JOB_LINK_FORMAT = "http://go/${CLUSTER}/${ROLE}/${ENVIRON}/${TOPOLOGY}";
    final String SUBSTITUTED_JOB_LINK = "http://go/local/heron/test/test_topology";

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(AuroraContext.JOB_LINK_TEMPLATE))
        .thenReturn(JOB_LINK_FORMAT);

    scheduler.initialize(mockConfig, Mockito.mock(Config.class));

    PowerMockito.spy(Misc.class);
    PowerMockito.doReturn(SUBSTITUTED_JOB_LINK)
        .when(Misc.class, "substitute", mockConfig, JOB_LINK_FORMAT);

    List<String> result = scheduler.getJobLinks();

    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).equals(SUBSTITUTED_JOB_LINK));
  }
}
