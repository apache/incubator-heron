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

package org.apache.heron.scheduler.slurm;

import java.util.HashSet;
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

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({SlurmContext.class, SchedulerUtils.class})
public class SlurmSchedulerTest {
  private static final String SLURM_PATH = "path.heron";
  private static final String SLURM_ID_FILE = "slurm_job.id";
  private static final String PACKING_PLAN_ID = "packing.plan.id";
  private static final String CONTAINER_ID = "packing.container.id";
  private static final String TOPOLOGY_NAME = "topologyName";
  private static final String CLUSTER = "testCluster";
  private static final String ROLE = "testRole";
  private static final String ENVIRON = "testEnviron";
  private static final String WORKING_DIRECTORY = "workingDirectory";

  private static SlurmScheduler scheduler;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    scheduler = Mockito.spy(SlurmScheduler.class);
    Mockito.doReturn(new String[]{}).when(
        scheduler).getExecutorCommand(
        Mockito.any(PackingPlan.class));
    Mockito.doReturn(SLURM_PATH).when(scheduler).getHeronSlurmPath();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    scheduler.close();
  }

  private static Config createRunnerConfig() {
    Config config = Config.newBuilder()
        .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
        .put(Key.CLUSTER, CLUSTER)
        .put(Key.ROLE, ROLE)
        .put(Key.ENVIRON, ENVIRON).build();
    return config;
  }

  /**
   * Test the schedule method
   * @throws Exception
   */
  @Test
  public void testOnSchedule() throws Exception {
    SlurmController controller = Mockito.mock(SlurmController.class);
    Mockito.doReturn(controller).when(scheduler).getController();

    Config config = createRunnerConfig();
    Config runtime = Mockito.mock(Config.class);

    PowerMockito.spy(SlurmContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(SlurmContext.class, "workingDirectory",
        config);

    scheduler.initialize(config, runtime);

    // Fail to schedule due to null PackingPlan
    Assert.assertFalse(scheduler.onSchedule(null));

    PackingPlan plan = new PackingPlan(PACKING_PLAN_ID, new HashSet<PackingPlan.ContainerPlan>());
    Assert.assertTrue(plan.getContainers().isEmpty());
    // Fail to schedule due to PackingPlan is empty
    Assert.assertFalse(scheduler.onSchedule(plan));

    // Construct valid PackingPlan
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan = new PackingPlan(PACKING_PLAN_ID, containers);

    // Failed to create job via controller
    Mockito.doReturn(false).when(
        controller).createJob(Mockito.anyString(), Mockito.anyString(),
        Matchers.any(String[].class), Mockito.anyString(), Mockito.anyInt());
    Assert.assertFalse(scheduler.onSchedule(validPlan));
    Mockito.verify(controller).createJob(Mockito.eq(SLURM_PATH),
        Mockito.anyString(), Matchers.any(String[].class),
        Mockito.anyString(), Mockito.anyInt());

    // Happy path
    Mockito.doReturn(true).when(
        controller).createJob(Mockito.anyString(), Mockito.anyString(),
        Matchers.any(String[].class), Mockito.anyString(), Mockito.anyInt());
    Assert.assertTrue(scheduler.onSchedule(validPlan));
    Mockito.verify(
        controller, Mockito.times(2)).createJob(Mockito.anyString(), Mockito.anyString(),
        Matchers.any(String[].class), Mockito.anyString(), Mockito.anyInt());
  }

  @Test
  public void testOnKill() throws Exception {
    SlurmController controller = Mockito.mock(SlurmController.class);
    Mockito.doReturn(controller).when(scheduler).getController();
    Mockito.doReturn(SLURM_ID_FILE).when(scheduler).getJobIdFilePath();

    Config config = createRunnerConfig();
    Config runtime = Mockito.mock(Config.class);

    PowerMockito.spy(SlurmContext.class);
    PowerMockito.doReturn(WORKING_DIRECTORY).when(SlurmContext.class, "workingDirectory",
        config);
    scheduler.initialize(config, runtime);
    // Failed to kill job via controller
    Mockito.doReturn(false).when(
        controller).killJob(Mockito.anyString());
    Assert.assertFalse(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller).killJob(Mockito.anyString());
    // Happy path
    Mockito.doReturn(true).when(
        controller).killJob(Mockito.anyString());
    Assert.assertTrue(scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance()));
    Mockito.verify(controller, Mockito.times(2)).killJob(Mockito.anyString());
  }
}
