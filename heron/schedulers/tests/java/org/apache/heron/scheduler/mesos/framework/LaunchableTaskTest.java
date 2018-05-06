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
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.mesos.Protos;

public class LaunchableTaskTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String ROLE = "role";
  private static final long NUM_CONTAINER = 2;
  private static final String TOPOLOGY_PACKAGE_URI = "topologyPackageURI";
  private static final String CORE_PACKAGE_URI = "corePackageURI";

  private Config config;
  private Config runtime;

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

  }

  @After
  public void after() throws Exception {
  }

  /**
   * Unit test for constructMesosTaskInfo()
   */
  @Test
  public void testConstructMesosTaskInfo() throws Exception {
    // Create a valid offer
    final double CPU = 0.5;
    final double MEM = 1.6;
    final double DISK = 2.7;
    Protos.Offer.Builder builder =
        Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("id"))
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
    Protos.Resource mem =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.MEM_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(MEM))
            .build();
    Protos.Resource disk =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.DISK_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(DISK))
            .build();
    builder.addResources(cpu).addResources(mem).addResources(disk);

    Protos.Offer offer = builder.build();

    // Create a BaseContainer
    final int containerIndex = 0;
    BaseContainer container = new BaseContainer();
    container.name = TaskUtils.getTaskNameForContainerIndex(containerIndex);

    container.runAsUser = ROLE;
    container.description = String.format("Container %d for topology %s",
        containerIndex, TOPOLOGY_NAME);
    container.cpu = CPU;
    container.diskInMB = DISK;
    container.memInMB = MEM;
    container.ports = SchedulerUtils.ExecutorPort.getRequiredPorts().size();
    container.shell = true;
    container.retries = Integer.MAX_VALUE;
    container.dependencies = new ArrayList<>();
    String topologyPath = TOPOLOGY_PACKAGE_URI;
    String heronCoreReleasePath = CORE_PACKAGE_URI;

    container.dependencies.add(topologyPath);
    container.dependencies.add(heronCoreReleasePath);

    // List of free ports
    List<Integer> freePorts = new ArrayList<>();
    for (int i = 0; i < SchedulerUtils.ExecutorPort.getRequiredPorts().size(); i++) {
      freePorts.add(i);
    }

    // Instantiate the LaunchableTask
    final String taskName = TaskUtils.getTaskNameForContainerIndex(containerIndex);
    final String taskId = TaskUtils.getTaskId(taskName, 0);
    LaunchableTask launchableTask =
        Mockito.spy(new LaunchableTask(taskId, container, offer, freePorts));
    final String executorCmd = "executor_command";
    Mockito.doReturn(executorCmd).when(launchableTask).executorCommand(
        config, runtime, containerIndex);

    Protos.TaskInfo taskInfo = launchableTask.constructMesosTaskInfo(config, runtime);
    Assert.assertTrue(taskInfo.isInitialized());
    Assert.assertEquals("slave-id", taskInfo.getSlaveId().getValue());
    int resourceCount = 0;
    for (Protos.Resource r : taskInfo.getResourcesList()) {
      Assert.assertEquals("*", r.getRole());
      if (r.getName().equals(TaskResources.CPUS_RESOURCE_NAME)) {
        Assert.assertEquals(Protos.Value.Type.SCALAR, r.getType());
        Assert.assertEquals(CPU, r.getScalar().getValue(), 0.01);
        resourceCount++;
      }

      if (r.getName().equals(TaskResources.MEM_RESOURCE_NAME)) {
        Assert.assertEquals(Protos.Value.Type.SCALAR, r.getType());
        Assert.assertEquals(MEM, r.getScalar().getValue(), 0.01);
        resourceCount++;
      }

      if (r.getName().equals(TaskResources.DISK_RESOURCE_NAME)) {
        Assert.assertEquals(Protos.Value.Type.SCALAR, r.getType());
        Assert.assertEquals(DISK, r.getScalar().getValue(), 0.01);
        resourceCount++;
      }

      if (r.getName().equals(TaskResources.PORT_RESOURCE_NAME)) {
        Assert.assertEquals(Protos.Value.Type.RANGES, r.getType());
        Assert.assertEquals(0, r.getRanges().getRange(0).getBegin());
        Assert.assertEquals(freePorts.size() - 1, r.getRanges().getRange(0).getEnd());
        resourceCount++;
      }
    }
    Assert.assertEquals(4, resourceCount);

    Protos.CommandInfo commandInfo = taskInfo.getCommand();
    Assert.assertEquals(executorCmd, commandInfo.getValue());
    Assert.assertEquals(ROLE, commandInfo.getUser());
    Assert.assertEquals(2, commandInfo.getUrisCount());
    Assert.assertEquals(TOPOLOGY_PACKAGE_URI, commandInfo.getUris(0).getValue());
    Assert.assertTrue(commandInfo.getUris(0).getExtract());
    Assert.assertEquals(CORE_PACKAGE_URI, commandInfo.getUris(1).getValue());
    Assert.assertTrue(commandInfo.getUris(1).getExtract());


    // Test join
    String[] s = {"A", "B", "C"};
    String joined = launchableTask.join(s, "--");
    Assert.assertEquals("A--B--C", joined);
  }
}
