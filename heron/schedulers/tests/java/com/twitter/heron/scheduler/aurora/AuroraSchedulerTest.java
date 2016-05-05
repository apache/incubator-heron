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
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;

public class AuroraSchedulerTest {
  private static final String AURORA_PATH = "path.aurora";
  private static final String PACKING_PLAN_ID = "packing.plan.id";
  private static final String CONTAINER_ID = "packing.container.id";
  private static final String TOPOLOGY_NAME = "topologyName";

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
    Mockito.doReturn(AURORA_PATH).when(scheduler).getHeronAuroraPath();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  @Test
  public void testOnSchedule() throws Exception {
    AuroraController controller = Mockito.mock(AuroraController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan plan =
        new PackingPlan(
            PACKING_PLAN_ID,
            new HashMap<String, PackingPlan.ContainerPlan>(),
            Mockito.mock(PackingPlan.Resource.class));
    Assert.assertTrue(plan.containers.isEmpty());
    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(plan));

    // Construct valid PackingPlan
    Map<String, PackingPlan.ContainerPlan> containers = new HashMap<>();
    containers.put(CONTAINER_ID, Mockito.mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan =
        new PackingPlan(PACKING_PLAN_ID, containers, Mockito.mock(PackingPlan.Resource.class));

    // Failed to create job via controller
    Mockito.doReturn(false).when(
        controller).createJob(Mockito.anyString(), Matchers.anyMapOf(String.class, String.class));
    Assert.assertFalse(scheduler.onSchedule(validPlan));
    Mockito.verify(controller).createJob(Mockito.eq(AURORA_PATH),
        Matchers.anyMapOf(String.class, String.class));

    // Happy path
    Mockito.doReturn(true).when(
        controller).createJob(Mockito.anyString(), Matchers.anyMapOf(String.class, String.class));
    Assert.assertTrue(scheduler.onSchedule(validPlan));
    Mockito.verify(
        controller, Mockito.times(2)).createJob(Mockito.eq(AURORA_PATH),
        Matchers.anyMapOf(String.class, String.class));
  }

  @Test
  public void testOnKill() throws Exception {
    AuroraController controller = Mockito.mock(AuroraController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    scheduler.initialize(Mockito.mock(Config.class), Mockito.mock(Config.class));

    // Failed to kill job via controller
    Mockito.doReturn(false).when(
        controller).killJob();
    Assert.assertFalse(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller).killJob();

    // Happy path
    Mockito.doReturn(true).when(
        controller).killJob();
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
        controller).restartJob(containerToRestart);
    Assert.assertFalse(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller).restartJob(containerToRestart);

    // Happy path
    Mockito.doReturn(true).when(
        controller).restartJob(containerToRestart);
    Assert.assertTrue(scheduler.onRestart(restartTopologyRequest));
    Mockito.verify(controller, Mockito.times(2)).restartJob(containerToRestart);
  }
}
