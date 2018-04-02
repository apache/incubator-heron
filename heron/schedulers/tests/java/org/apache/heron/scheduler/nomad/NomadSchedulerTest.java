//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package org.apache.heron.scheduler.nomad;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.hashicorp.nomad.apimodel.Job;
import com.hashicorp.nomad.apimodel.Task;
import com.hashicorp.nomad.apimodel.TaskGroup;
import com.hashicorp.nomad.javasdk.NomadApiClient;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyVararg;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NomadScheduler.class, Job.class, SchedulerUtils.class})
@PowerMockIgnore("javax.net.ssl.*")
public class NomadSchedulerTest {
  private static final Logger LOG = Logger.getLogger(NomadSchedulerTest.class.getName());


  private static final String TOPOLOGY_NAME = "topology-name";
  private static final String TOPOLOGY_ID = "topology-id";
  private static final int CONTAINER_INDEX = 1;
  private static final String PACKING_PLAN_ID = "packing_plan_id";
  private static final String SCHEDULER_URI = "http://127.0.0.1:4646";
  private static final String[] EXECUTOR_CMD_ARGS = {"args1", "args2"};
  private static final String GROUP_NAME = "group-name";
  private static final String TASK_NAME = "task-name";
  private static final String TOPOLOGY_DOWNLOAD_CMD = "topology-download-cmd";
  private static final String HERON_NOMAD_SCRIPT = "heron_nomad_script";
  private static final double CPU_RESOURCE = 100.0;
  private static final ByteAmount MEMORY_RESOURCE = ByteAmount.fromMegabytes(100);
  private static final ByteAmount DISK_RESOURCE = ByteAmount.fromMegabytes(1000);
  private static final int HERON_NOMAD_CORE_FREQ_MAPPING = 1000;
  private static final String CORE_PACKAGE_URI = "core-package-uri";
  private static final Boolean USE_CORE_PACKAGE_URI = true;
  private static final String EXECUTOR_BINARY = "executor-binary";

  private static NomadScheduler scheduler;

  private Config mockRuntime;
  private Config mockConfig;


  @Before
  public void setUp() throws Exception {
    Config config = Config.newBuilder()
        .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
        .put(Key.TOPOLOGY_ID, TOPOLOGY_ID)
        .put(NomadContext.HERON_NOMAD_SCHEDULER_URI, SCHEDULER_URI)
        .put(NomadContext.HERON_NOMAD_CORE_FREQ_MAPPING, HERON_NOMAD_CORE_FREQ_MAPPING)
        .put(Key.CORE_PACKAGE_URI, CORE_PACKAGE_URI)
        .put(Key.USE_CORE_PACKAGE_URI, USE_CORE_PACKAGE_URI)
        .put(Key.EXECUTOR_BINARY, EXECUTOR_BINARY)
        .put(NomadContext.HERON_NOMAD_DRIVER, NomadConstants.NomadDriver.RAW_EXEC.getName())
        .build();

    this.mockRuntime = config;
    this.mockConfig = config;

    scheduler = spy(NomadScheduler.class);
  }

  @After
  public void after() throws Exception {
    scheduler.close();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Test
  public void testOnSchedule() throws Exception {
    PowerMockito.mockStatic(NomadScheduler.class);

    doReturn(new LinkedList<>()).when(scheduler)
        .getJobs(any(PackingPlan.class));

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    // Fail to schedule due to null PackingPlan
    assertFalse(scheduler.onSchedule(null));

    PackingPlan pplan =
        new PackingPlan(
            PACKING_PLAN_ID,
            new HashSet<>()
        );
    assertTrue(pplan.getContainers().isEmpty());

    // Fail to schedule due to PackingPlan is empty
    assertFalse(scheduler.onSchedule(pplan));

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(mock(PackingPlan.ContainerPlan.class));
    PackingPlan validPlan =
        new PackingPlan(PACKING_PLAN_ID, containers);

    // Fail to submit due to client failure
    Job[] jobs = {new Job(), new Job(), new Job()};
    List<Job> jobList = Arrays.asList(jobs);
    doReturn(jobList).when(scheduler).getJobs(validPlan);
    PowerMockito.doThrow(new RuntimeException()).when(NomadScheduler.class,
        "startJobs", any(NomadApiClient.class), anyVararg());
    assertFalse(scheduler.onSchedule(validPlan));

    // Succeed
    doReturn(jobList).when(scheduler).getJobs(validPlan);
    PowerMockito.doNothing().when(NomadScheduler.class, "startJobs",
        any(NomadApiClient.class), anyVararg());
    assertTrue(scheduler.onSchedule(validPlan));
  }

  @Test
  public void testOnRestart() throws Exception {
    PowerMockito.mockStatic(NomadScheduler.class);

    // Construct RestartTopologyRequest to restart container
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME)
            .setContainerIndex(CONTAINER_INDEX)
            .build();

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    // Fail to restart
    PowerMockito.when(NomadScheduler.getTopologyContainerJob(
        any(NomadApiClient.class),
        anyString(), anyInt())).thenReturn(new Job());
    PowerMockito.doThrow(new RuntimeException()).when(
        NomadScheduler.class, "restartJobs", any(NomadApiClient.class), any(Job.class));
    assertFalse(scheduler.onRestart(restartTopologyRequest));

    // Succeed to restart
    PowerMockito.when(NomadScheduler.getTopologyContainerJob(
        any(NomadApiClient.class),
        anyString(), anyInt())).thenReturn(new Job());
    PowerMockito.doNothing().when(NomadScheduler.class, "restartJobs",
        any(NomadApiClient.class), any(Job.class));
    assertTrue(scheduler.onRestart(restartTopologyRequest));

    // Construct RestartTopologyRequest to restart whole topology
    restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setTopologyName(TOPOLOGY_NAME)
            .setContainerIndex(-1)
            .build();

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    Job[] jobs = {new Job(), new Job(), new Job()};
    List<Job> jobList = Arrays.asList(jobs);

    // Fail to restart
    PowerMockito.when(NomadScheduler.getTopologyJobs(any(NomadApiClient.class),
        anyString())).thenReturn(jobList);
    PowerMockito.doThrow(new RuntimeException()).when(NomadScheduler.class,
        "restartJobs", any(), anyVararg());
    assertFalse(scheduler.onRestart(restartTopologyRequest));

    // Succeed to restart
    PowerMockito.when(NomadScheduler.getTopologyJobs(any(NomadApiClient.class),
        anyString())).thenReturn(jobList);
    PowerMockito.doNothing().when(NomadScheduler.class, "restartJobs",
        any(NomadApiClient.class), anyVararg());
    assertTrue(scheduler.onRestart(restartTopologyRequest));
  }

  @Test
  public void testOnKill() throws Exception {
    PowerMockito.mockStatic(NomadScheduler.class);


    Scheduler.KillTopologyRequest killTopologyRequest
        = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(TOPOLOGY_NAME)
        .build();

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    Job[] jobs = {new Job(), new Job(), new Job()};
    List<Job> jobList = Arrays.asList(jobs);

    // Fail to kill
    PowerMockito.when(NomadScheduler.getTopologyJobs(any(NomadApiClient.class),
        anyString())).thenReturn(jobList);
    PowerMockito.doThrow(new RuntimeException()).when(NomadScheduler.class, "killJobs",
        any(NomadApiClient.class), anyVararg());
    assertFalse(scheduler.onKill(killTopologyRequest));

    // Succeed to kill
    PowerMockito.when(NomadScheduler.getTopologyJobs(any(NomadApiClient.class),
        anyString())).thenReturn(jobList);
    PowerMockito.doNothing().when(NomadScheduler.class, "killJobs",
        any(NomadApiClient.class), anyVararg());
    assertTrue(scheduler.onKill(killTopologyRequest));
  }

  @Test
  public void testGetJobLinks() {
    PowerMockito.mockStatic(NomadScheduler.class);

    final String JOB_LINK = SCHEDULER_URI + "/ui/jobs";
    scheduler.initialize(this.mockConfig, this.mockRuntime);
    List<String> links = scheduler.getJobLinks();
    assertEquals(1, links.size());
    assertTrue(links.get(0).equals(JOB_LINK));
  }

  @Test
  public void testGetJob() {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(mock(PackingPlan.ContainerPlan.class));

    PackingPlan.ContainerPlan containerPlan = new PackingPlan.ContainerPlan(
        CONTAINER_INDEX, new HashSet<>(), mock(Resource.class));
    Optional<PackingPlan.ContainerPlan> plan = Optional.of(containerPlan);

    Resource resource = new Resource(CPU_RESOURCE, MEMORY_RESOURCE, DISK_RESOURCE);

    scheduler.initialize(this.mockConfig, this.mockRuntime);
    doReturn(new TaskGroup()).when(scheduler).getTaskGroup(
        anyString(), anyInt(), any());

    Job job = scheduler.getJob(CONTAINER_INDEX, plan, resource);
    LOG.info("job: " + job);

    assertEquals(TOPOLOGY_ID + "-" + CONTAINER_INDEX, job.getId());
    assertEquals(TOPOLOGY_NAME + "-" + CONTAINER_INDEX, job.getName());
    assertArrayEquals(Arrays.asList(NomadConstants.NOMAD_DEFAULT_DATACENTER).toArray(),
        job.getDatacenters().toArray());

    assertNotNull(job.getTaskGroups());
  }

  @Test
  public void testGetTaskGroup() {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(mock(PackingPlan.ContainerPlan.class));
    Resource resource = new Resource(CPU_RESOURCE, MEMORY_RESOURCE, DISK_RESOURCE);

    PackingPlan.ContainerPlan containerPlan = new PackingPlan.ContainerPlan(
        CONTAINER_INDEX, new HashSet<>(), mock(Resource.class));

    scheduler.initialize(this.mockConfig, this.mockRuntime);
    doReturn(new Task()).when(scheduler).getTask(
        anyString(), anyInt(), any());

    TaskGroup taskGroup = scheduler.getTaskGroup(GROUP_NAME, CONTAINER_INDEX, resource);
    LOG.info("taskGroup: " + taskGroup);

    assertEquals(GROUP_NAME, taskGroup.getName());
    assertNotNull(taskGroup.getCount());
    assertNotNull(taskGroup.getTasks());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetTaskRawExec() {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(mock(PackingPlan.ContainerPlan.class));

    PowerMockito.mockStatic(SchedulerUtils.class);

    Resource resource = new Resource(CPU_RESOURCE, MEMORY_RESOURCE, DISK_RESOURCE);

    PowerMockito.when(SchedulerUtils.executorCommandArgs(
        any(), any(), anyMap(), anyString()))
        .thenReturn(EXECUTOR_CMD_ARGS);

    PowerMockito.mockStatic(NomadScheduler.class);
    PowerMockito.when(NomadScheduler.getFetchCommand(any(), any()))
        .thenReturn(TOPOLOGY_DOWNLOAD_CMD);
    PowerMockito.when(NomadScheduler.getHeronNomadScript(this.mockConfig))
        .thenReturn(HERON_NOMAD_SCRIPT);
    PowerMockito.when(NomadScheduler.longToInt(MEMORY_RESOURCE.asMegabytes()))
        .thenReturn((int) MEMORY_RESOURCE.asMegabytes());
    PowerMockito.when(NomadScheduler.longToInt(DISK_RESOURCE.asMegabytes()))
        .thenReturn((int) DISK_RESOURCE.asMegabytes());

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    Task task = scheduler.getTask(TASK_NAME, CONTAINER_INDEX, resource);
    LOG.info("task: " + task);

    assertEquals(TASK_NAME, task.getName());
    assertEquals(NomadConstants.NomadDriver.RAW_EXEC.getName(), task.getDriver());
    assertTrue(task.getConfig().containsKey(NomadConstants.NOMAD_TASK_COMMAND));
    assertEquals(NomadConstants.SHELL_CMD,
        task.getConfig().get(NomadConstants.NOMAD_TASK_COMMAND));
    assertTrue(task.getConfig().containsKey(NomadConstants.NOMAD_TASK_COMMAND_ARGS));
    assertArrayEquals(Arrays.asList(NomadConstants.NOMAD_HERON_SCRIPT_NAME).toArray(),
        (String[]) task.getConfig().get(NomadConstants.NOMAD_TASK_COMMAND_ARGS));
    assertEquals(1, task.getTemplates().size());
    assertEquals(HERON_NOMAD_SCRIPT, task.getTemplates().get(0).getEmbeddedTmpl());
    assertEquals(NomadConstants.NOMAD_HERON_SCRIPT_NAME,
        task.getTemplates().get(0).getDestPath());

    assertEquals((int) CPU_RESOURCE * HERON_NOMAD_CORE_FREQ_MAPPING,
        task.getResources().getCpu().intValue());
    assertEquals((int) MEMORY_RESOURCE.asMegabytes(),
        task.getResources().getMemoryMb().intValue());
    assertEquals((int) DISK_RESOURCE.asMegabytes(),
        task.getResources().getDiskMb().intValue());
    assertTrue(task.getEnv().containsKey(NomadConstants.HERON_NOMAD_WORKING_DIR));
    assertTrue(task.getEnv().containsKey(NomadConstants.HERON_USE_CORE_PACKAGE_URI));
    assertTrue(task.getEnv().containsKey(NomadConstants.HERON_CORE_PACKAGE_URI));
    assertTrue(task.getEnv().containsKey(NomadConstants.HERON_TOPOLOGY_DOWNLOAD_CMD));
    assertTrue(task.getEnv().containsKey(NomadConstants.HERON_EXECUTOR_CMD));

    assertEquals(NomadKey.WORKING_DIRECTORY.getDefaultString() + "/container-"
            + String.valueOf(CONTAINER_INDEX),
        task.getEnv().get(NomadConstants.HERON_NOMAD_WORKING_DIR));
    assertEquals(USE_CORE_PACKAGE_URI.toString(),
        task.getEnv().get(NomadConstants.HERON_USE_CORE_PACKAGE_URI));
    assertEquals(CORE_PACKAGE_URI,
        task.getEnv().get(NomadConstants.HERON_CORE_PACKAGE_URI));
    assertEquals(TOPOLOGY_DOWNLOAD_CMD,
        task.getEnv().get(NomadConstants.HERON_TOPOLOGY_DOWNLOAD_CMD));
    assertEquals("./heron-core/bin/heron-executor args1 args2",
        task.getEnv().get(NomadConstants.HERON_EXECUTOR_CMD));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetTaskDocker() {

    this.mockRuntime = this.mockRuntime.newBuilder()
        .put(NomadContext.HERON_NOMAD_DRIVER, NomadConstants.NomadDriver.DOCKER.getName())
        .build();

    this.mockConfig = this.mockConfig.newBuilder()
        .put(NomadContext.HERON_NOMAD_DRIVER, NomadConstants.NomadDriver.DOCKER.getName())
        .build();

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(mock(PackingPlan.ContainerPlan.class));

    PowerMockito.mockStatic(SchedulerUtils.class);

    Resource resource = new Resource(CPU_RESOURCE, MEMORY_RESOURCE, DISK_RESOURCE);

    PowerMockito.when(SchedulerUtils.executorCommandArgs(
        any(), any(), anyMap(), anyString()))
        .thenReturn(EXECUTOR_CMD_ARGS);

    PowerMockito.mockStatic(NomadScheduler.class);
    PowerMockito.when(NomadScheduler.getFetchCommand(any(), any()))
        .thenReturn(TOPOLOGY_DOWNLOAD_CMD);
    PowerMockito.when(NomadScheduler.getHeronNomadScript(this.mockConfig))
        .thenReturn(HERON_NOMAD_SCRIPT);
    PowerMockito.when(NomadScheduler.longToInt(MEMORY_RESOURCE.asMegabytes()))
        .thenReturn((int) MEMORY_RESOURCE.asMegabytes());
    PowerMockito.when(NomadScheduler.longToInt(DISK_RESOURCE.asMegabytes()))
        .thenReturn((int) DISK_RESOURCE.asMegabytes());

    scheduler.initialize(this.mockConfig, this.mockRuntime);

    Task task = scheduler.getTask(TASK_NAME, CONTAINER_INDEX, resource);
    LOG.info("task: " + task);

    assertEquals(TASK_NAME, task.getName());
    assertEquals(NomadConstants.NomadDriver.DOCKER.getName(), task.getDriver());
    assertTrue(task.getConfig().containsKey(NomadConstants.NOMAD_TASK_COMMAND));
    assertEquals(NomadConstants.SHELL_CMD,
        task.getConfig().get(NomadConstants.NOMAD_TASK_COMMAND));

    assertEquals((int) CPU_RESOURCE * HERON_NOMAD_CORE_FREQ_MAPPING,
        task.getResources().getCpu().intValue());
    assertEquals((int) MEMORY_RESOURCE.asMegabytes(),
        task.getResources().getMemoryMb().intValue());
    assertEquals((int) DISK_RESOURCE.asMegabytes(),
        task.getResources().getDiskMb().intValue());
    assertTrue(task.getEnv().containsKey(NomadConstants.HOST));

    assertEquals("${attr.unique.network.ip-address}",
        task.getEnv().get(NomadConstants.HOST));
  }
}
