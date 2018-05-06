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

package org.apache.heron.scheduler.marathon;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;

public class MarathonSchedulerTest {
  private static final String TOPOLOGY_CONF = "topology_conf";
  private static final String TOPOLOGY_NAME = "topology_name";
  private static final String CONTAINER_ID = "container_id";
  private static final int CONTAINER_INDEX = 1;
  private static final String PACKING_PLAN_ID = "packing_plan_id";
  private static final String EXECUTOR_CMD = "executor_cmd";

  private static MarathonScheduler scheduler;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    scheduler = Mockito.spy(MarathonScheduler.class);
    Mockito.doReturn(EXECUTOR_CMD).when(scheduler)
        .getExecutorCommand(Mockito.anyInt());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  @Test
  public void testOnSchedule() throws Exception {
    MarathonController controller = Mockito.mock(MarathonController.class);
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
    Mockito.doReturn(false).when(controller).submitTopology(Mockito.anyString());
    Assert.assertFalse(scheduler.onSchedule(validPlan));
    Mockito.verify(controller).submitTopology(Matchers.anyString());

    // Succeed to submit topology
    Mockito.doReturn(true).when(controller).submitTopology(Mockito.anyString());
    Assert.assertTrue(scheduler.onSchedule(validPlan));
    Mockito.verify(controller, Mockito.times(2)).submitTopology(Matchers.anyString());
  }

  @Test
  public void testOnRestart() throws Exception {
    MarathonController controller = Mockito.mock(MarathonController.class);
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
    MarathonController controller = Mockito.mock(MarathonController.class);
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
  public void testGetJobLinks() throws Exception {
    final String SCHEDULER_URI = "http://marathon.scheduler.uri";
    final String JOB_LINK = SCHEDULER_URI + MarathonConstants.JOB_LINK + TOPOLOGY_NAME;

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(MarathonContext.HERON_MARATHON_SCHEDULER_URI))
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
