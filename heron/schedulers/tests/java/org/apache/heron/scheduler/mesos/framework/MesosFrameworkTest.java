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

package org.apache.heron.scheduler.mesos.framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

public class MesosFrameworkTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String ROLE = "role";
  private static final long NUM_CONTAINER = 2;
  private static final int CONTAINER_INDEX = 0;
  private static final String TOPOLOGY_PACKAGE_URI = "topologyPackageURI";
  private static final String CORE_PACKAGE_URI = "corePackageURI";

  private Config config;
  private Config runtime;

  private MesosFramework mesosFramework;

  protected Protos.Offer getOffer() {
    final double CPU = 1;
    Protos.Offer.Builder builder =
        Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
            .setHostname("hostname")
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"));

    Protos.Resource cpu =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.CPUS_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(CPU))
            .build();

    Protos.Resource port =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.RANGES)
            .setName(TaskResources.PORT_RESOURCE_NAME)
            .setRanges(Protos.Value.Ranges.newBuilder()
                .addRange(Protos.Value.Range.newBuilder().setBegin(0).setEnd(5)))
            .build();

    builder.addResources(cpu).addResources(port);

    Protos.Offer offer = builder.build();
    return offer;
  }

  public Map<Protos.Offer, TaskResources> getOfferResources(Protos.Offer offer) {
    TaskResources resources = TaskResources.apply(offer, "*");

    Map<Protos.Offer, TaskResources> offerResources = new HashMap<>();
    offerResources.put(offer, resources);

    return offerResources;
  }

  @Before
  public void before() throws Exception {
    config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    Mockito.when(config.getStringValue(Key.ROLE)).thenReturn(ROLE);
    Mockito.when(config.getStringValue(Key.CORE_PACKAGE_URI)).thenReturn(CORE_PACKAGE_URI);

    runtime = Mockito.mock(Config.class);
    Mockito.when(runtime.getLongValue(Key.NUM_CONTAINERS)).thenReturn(NUM_CONTAINER);
    Properties properties = new Properties();
    properties.put(Key.TOPOLOGY_PACKAGE_URI.value(), TOPOLOGY_PACKAGE_URI);
    Mockito.when(runtime.get(Key.SCHEDULER_PROPERTIES)).thenReturn(properties);

    mesosFramework = Mockito.spy(new MesosFramework(config, runtime));
    SchedulerDriver driver = Mockito.mock(SchedulerDriver.class);

    // Register the mesos framework
    Protos.FrameworkID frameworkID =
        Protos.FrameworkID.newBuilder().setValue("framework-id").build();
    mesosFramework.registered(driver, frameworkID, Protos.MasterInfo.getDefaultInstance());
  }

  @After
  public void after() throws Exception {
    mesosFramework.getSchedulerDriver().stop();
  }

  @Test
  public void testCreateJob() throws Exception {
    BaseContainer container = new BaseContainer();
    container.name = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);

    Map<Integer, BaseContainer> jobDef = new HashMap<>();
    jobDef.put(CONTAINER_INDEX, container);

    Assert.assertTrue(mesosFramework.createJob(jobDef));

    Assert.assertEquals(1, mesosFramework.getContainersInfo().size());
    Assert.assertEquals(1, mesosFramework.getTasksId().size());
    Assert.assertEquals(1, mesosFramework.getToScheduleTasks().size());
    Assert.assertTrue(
        mesosFramework.getToScheduleTasks().peek().startsWith(
            String.format("container_%d", CONTAINER_INDEX)));
  }

  @Test
  public void testKillJob() throws Exception {
    String taskId = "task_id";
    mesosFramework.getToScheduleTasks().add(taskId);
    mesosFramework.getTasksId().put(CONTAINER_INDEX, taskId);

    Assert.assertTrue(mesosFramework.killJob());
    Assert.assertTrue(mesosFramework.getToScheduleTasks().isEmpty());
    Assert.assertTrue(mesosFramework.getTasksId().isEmpty());
    Assert.assertTrue(mesosFramework.isTerminated());

    Mockito.verify(mesosFramework.getSchedulerDriver())
        .killTask(Protos.TaskID.newBuilder().setValue(taskId).build());
  }

  @Test
  public void testRestartSingleContainer() throws Exception {
    String taskId = "task_id";
    mesosFramework.getTasksId().put(CONTAINER_INDEX, taskId);

    Assert.assertTrue(mesosFramework.restartJob(CONTAINER_INDEX));
    Assert.assertFalse(mesosFramework.isTerminated());

    Mockito.verify(mesosFramework.getSchedulerDriver())
        .killTask(Protos.TaskID.newBuilder().setValue(taskId).build());
  }

  @Test
  public void testRestartAllContainer() throws Exception {
    String taskId = "task_id";
    mesosFramework.getTasksId().put(CONTAINER_INDEX, taskId);

    // -1 means to restart all containers
    Assert.assertTrue(mesosFramework.restartJob(-1));
    Assert.assertFalse(mesosFramework.isTerminated());

    Mockito.verify(mesosFramework.getSchedulerDriver())
        .killTask(Protos.TaskID.newBuilder().setValue(taskId).build());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeclineUnusedOffer() throws Exception {
    Protos.Offer offer = getOffer();

    List<LaunchableTask> tasksToLaunch = new ArrayList<>();
    Mockito.doReturn(tasksToLaunch).
        when(mesosFramework).generateLaunchableTasks(Mockito.any(Map.class));

    List<Protos.Offer> offers = new ArrayList<>();
    offers.add(offer);
    mesosFramework.resourceOffers(mesosFramework.getSchedulerDriver(), offers);

    // The unused offer should be declined
    Mockito.verify(mesosFramework.getSchedulerDriver()).declineOffer(offer.getId());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConsumeOffer() throws Exception {
    Protos.Offer offer = getOffer();

    List<LaunchableTask> tasksToLaunch = new ArrayList<>();
    LaunchableTask launchableTask =
        new LaunchableTask("", new BaseContainer(), offer, new ArrayList<Integer>());
    tasksToLaunch.add(launchableTask);
    Mockito.doReturn(tasksToLaunch).
        when(mesosFramework).generateLaunchableTasks(Mockito.any(Map.class));
    Mockito.doNothing().when(mesosFramework).launchTasks(tasksToLaunch);

    List<Protos.Offer> offers = new ArrayList<>();
    offers.add(offer);
    mesosFramework.resourceOffers(mesosFramework.getSchedulerDriver(), offers);

    // The offer should be used and not be declined
    Mockito.verify(mesosFramework.getSchedulerDriver(), Mockito.never())
        .declineOffer(offer.getId());
  }

  @Test
  public void testHandleFailure() throws Exception {
    String taskName = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);
    String oldTaskId = TaskUtils.getTaskId(taskName, 0);

    BaseContainer baseContainer = new BaseContainer();
    baseContainer.retries = Integer.MAX_VALUE;
    baseContainer.name = taskName;
    mesosFramework.getContainersInfo().put(CONTAINER_INDEX, baseContainer);

    mesosFramework.handleMesosFailure(oldTaskId);
    String newTaskId = TaskUtils.getTaskId(taskName, 1);

    Mockito.verify(mesosFramework).scheduleNewTask(newTaskId);
  }

  @Test
  public void testScheduleNewTask() throws Exception {
    String taskName = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);
    String newTaskId = TaskUtils.getTaskId(taskName, 0);

    mesosFramework.scheduleNewTask(newTaskId);

    Assert.assertEquals(1, mesosFramework.getTasksId().size());
    Assert.assertEquals(newTaskId, mesosFramework.getTasksId().get(CONTAINER_INDEX));
    Assert.assertEquals(1, mesosFramework.getToScheduleTasks().size());
    Assert.assertEquals(newTaskId, mesosFramework.getToScheduleTasks().peek());
  }

  @Test
  public void testGenerateLaunchableTasks() throws Exception {
    Protos.Offer offer = getOffer();
    Map<Protos.Offer, TaskResources> offerResources = getOfferResources(offer);

    BaseContainer baseContainer = new BaseContainer();
    baseContainer.cpu = 0.1;
    baseContainer.memInMB = 0;
    baseContainer.diskInMB = 0;
    baseContainer.ports = 0;

    String taskName = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);
    String taskId = TaskUtils.getTaskId(taskName, 0);

    mesosFramework.getToScheduleTasks().add(taskId);
    mesosFramework.getContainersInfo().put(CONTAINER_INDEX, baseContainer);

    List<LaunchableTask> tasks = mesosFramework.generateLaunchableTasks(offerResources);
    Assert.assertEquals(1, tasks.size());
    LaunchableTask task = tasks.get(0);

    Assert.assertEquals(offer, task.offer);
    Assert.assertEquals(baseContainer, task.baseContainer);
    Assert.assertEquals(taskId, task.taskId);
    // We don't request any ports
    Assert.assertTrue(task.freePorts.isEmpty());
  }

  @Test
  public void testInsufficientResourcesGenerateLaunchableTasks() throws Exception {
    Protos.Offer offer = getOffer();
    Map<Protos.Offer, TaskResources> offerResources = getOfferResources(offer);

    BaseContainer baseContainer = new BaseContainer();
    // request more CPU than provided
    baseContainer.cpu = 10000;
    baseContainer.memInMB = 0;
    baseContainer.diskInMB = 0;
    baseContainer.ports = 0;

    String taskName = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);
    String taskId = TaskUtils.getTaskId(taskName, 0);

    mesosFramework.getToScheduleTasks().add(taskId);
    mesosFramework.getContainersInfo().put(CONTAINER_INDEX, baseContainer);

    List<LaunchableTask> tasks = mesosFramework.generateLaunchableTasks(offerResources);
    // The provides resources could not meet the needed one
    Assert.assertEquals(0, tasks.size());
  }

  @Test
  public void testLaunchTasks() throws Exception {
    Protos.Offer offer = getOffer();
    String taskId = TaskUtils.getTaskId(
        TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX), 0);

    List<LaunchableTask> tasksToLaunch = new ArrayList<>();
    LaunchableTask launchableTask = Mockito.spy(
        new LaunchableTask(taskId, new BaseContainer(), offer, new ArrayList<Integer>()));
    Protos.TaskInfo pTask = Protos.TaskInfo.getDefaultInstance();
    Mockito.doReturn(pTask).when(launchableTask).constructMesosTaskInfo(config, runtime);
    tasksToLaunch.add(launchableTask);

    List<Protos.TaskInfo> mesosTasks = new LinkedList<>();
    mesosTasks.add(pTask);

    // Launch topology successfully
    Mockito.when(mesosFramework.getSchedulerDriver().launchTasks(
        Mockito.anyCollectionOf(Protos.OfferID.class),
        Mockito.anyCollectionOf(Protos.TaskInfo.class)))
        .thenReturn(Protos.Status.DRIVER_RUNNING);
    mesosFramework.launchTasks(tasksToLaunch);

    // Should not invoke handleMesosFailure
    Mockito.verify(mesosFramework, Mockito.never()).handleMesosFailure(taskId);
    // Verify the launching
    Mockito.verify(mesosFramework.getSchedulerDriver())
        .launchTasks(Arrays.asList(new Protos.OfferID[]{offer.getId()}),
            mesosTasks);

    // Failed to launch the tasks
    Mockito.when(mesosFramework.getSchedulerDriver().launchTasks(
        Mockito.anyCollectionOf(Protos.OfferID.class),
        Mockito.anyCollectionOf(Protos.TaskInfo.class)))
        .thenReturn(Protos.Status.DRIVER_NOT_STARTED);

    Mockito.doNothing().when(mesosFramework).handleMesosFailure(taskId);

    mesosFramework.launchTasks(tasksToLaunch);
    Mockito.verify(mesosFramework).handleMesosFailure(taskId);
  }
}
