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

package org.apache.heron.scheduler.mesos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.mesos.framework.BaseContainer;
import org.apache.heron.scheduler.mesos.framework.MesosFramework;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.mesos.SchedulerDriver;


public class MesosSchedulerTest {
  private static final String TOPOLOGY_NAME = "testTopology";
  private static final String ROLE = "role";
  private static final long NUM_CONTAINER = 2;
  private static final String TOPOLOGY_PACKAGE_URI = "topologyPackageURI";
  private static final String CORE_PACKAGE_URI = "corePackageURI";

  private MesosScheduler scheduler;
  private MesosFramework mesosFramework;

  private BaseContainer baseContainer;

  @Before
  public void before() throws Exception {

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    Mockito.when(config.getStringValue(Key.ROLE)).thenReturn(ROLE);
    Mockito.when(config.getStringValue(Key.CORE_PACKAGE_URI)).thenReturn(CORE_PACKAGE_URI);

    Config runtime = Mockito.mock(Config.class);
    Mockito.when(runtime.getLongValue(Key.NUM_CONTAINERS)).thenReturn(NUM_CONTAINER);
    Properties properties = new Properties();
    properties.put(Key.TOPOLOGY_PACKAGE_URI.value(), TOPOLOGY_PACKAGE_URI);
    Mockito.when(runtime.get(Key.SCHEDULER_PROPERTIES)).thenReturn(properties);

    mesosFramework = Mockito.mock(MesosFramework.class);
    SchedulerDriver driver = Mockito.mock(SchedulerDriver.class);
    baseContainer = Mockito.mock(BaseContainer.class);

    scheduler = Mockito.spy(MesosScheduler.class);
    Mockito.doReturn(mesosFramework).when(scheduler).getMesosFramework();
    Mockito.doReturn(driver).when(scheduler)
        .getSchedulerDriver(Mockito.anyString(), Mockito.eq(mesosFramework));
    Mockito.doNothing().when(scheduler)
        .startSchedulerDriver();

    scheduler.initialize(config, runtime);
  }

  @After
  public void after() throws Exception {
    scheduler.close();
  }

  @Test
  public void testOnSchedule() throws Exception {
    Mockito.doReturn(baseContainer).when(scheduler)
        .getBaseContainer(Mockito.anyInt(), Mockito.any(PackingPlan.class));

    scheduler.onSchedule(Mockito.mock(PackingPlan.class));

    Map<Integer, BaseContainer> expectedJobDef = new HashMap<>();
    for (int containerIndex = 0;
         containerIndex < NUM_CONTAINER;
         containerIndex++) {
      expectedJobDef.put(containerIndex, baseContainer);
    }

    Mockito.verify(mesosFramework).createJob(Mockito.eq(expectedJobDef));
  }

  @Test
  public void testOnKill() throws Exception {
    scheduler.onKill(Scheduler.KillTopologyRequest.getDefaultInstance());
    Mockito.verify(mesosFramework).killJob();
  }

  @Test
  public void testOnRestart() throws Exception {
    Scheduler.RestartTopologyRequest request =
        Scheduler.RestartTopologyRequest.newBuilder()
            .setContainerIndex(-1)
            .setTopologyName(TOPOLOGY_NAME).build();
    scheduler.onRestart(request);
    Mockito.verify(mesosFramework).restartJob(-1);
  }

  @Test
  public void testGetBaseContainer() throws Exception {
    final double CPU = 0.5;
    final ByteAmount MEM = ByteAmount.fromMegabytes(100);
    final ByteAmount DISK = ByteAmount.fromMegabytes(100);

    Resource containerResources = new Resource(CPU, MEM, DISK);
    PackingPlan.ContainerPlan containerPlan =
        new PackingPlan.ContainerPlan(
            0, new HashSet<PackingPlan.InstancePlan>(), containerResources);

    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    containerPlans.add(containerPlan);
    PackingPlan packingPlan = new PackingPlan(TOPOLOGY_NAME, containerPlans);

    BaseContainer container = scheduler.getBaseContainer(0, packingPlan);

    // Assert we have constructed the correct BaseContainer structure
    Assert.assertEquals(ROLE, container.runAsUser);
    Assert.assertEquals(CPU, container.cpu, 0.01);
    Assert.assertEquals(MEM, ByteAmount.fromMegabytes(((Double) container.memInMB).longValue()));
    Assert.assertEquals(DISK, ByteAmount.fromMegabytes(((Double) container.diskInMB).longValue()));
    Assert.assertEquals(SchedulerUtils.ExecutorPort.getRequiredPorts().size(), container.ports);
    Assert.assertEquals(2, container.dependencies.size());
    Assert.assertTrue(container.dependencies.contains(CORE_PACKAGE_URI));
    Assert.assertTrue(container.dependencies.contains(TOPOLOGY_PACKAGE_URI));
    Assert.assertTrue(container.environmentVariables.isEmpty());
    Assert.assertTrue(container.name.startsWith("container_0"));

    // Convert to JSON
    String str = container.toString();
    String json = BaseContainer.getJobDefinitionInJSON(container);
    Assert.assertEquals(json, str);

    // Convert the JSON back to BaseContainer
    BaseContainer newContainer = BaseContainer.getJobFromJSONString(json);
    Assert.assertEquals(json, BaseContainer.getJobDefinitionInJSON(newContainer));
  }
}
