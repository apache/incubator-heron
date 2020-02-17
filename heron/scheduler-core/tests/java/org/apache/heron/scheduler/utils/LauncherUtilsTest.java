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

package org.apache.heron.scheduler.utils;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.ReflectionUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({ReflectionUtils.class, TopologyUtils.class, TopologyAPI.Topology.class})
public class LauncherUtilsTest {
  @Test
  public void generatesPackingPlan() throws Exception {
    final String PACKING_CLASS = "nonExistingTestPackingClass";

    final PackingPlan mockPackingPlan = Mockito.mock(PackingPlan.class);

    IPacking mockPacking = Mockito.mock(IPacking.class);
    Mockito.when(mockPacking.pack()).thenReturn(mockPackingPlan);

    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(mockPacking).when(ReflectionUtils.class, "newInstance", PACKING_CLASS);

    TopologyAPI.Topology mockTopology = PowerMockito.mock(TopologyAPI.Topology.class);

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(Key.PACKING_CLASS)).thenReturn(PACKING_CLASS);
    Mockito.when(mockConfig.get(Key.TOPOLOGY_DEFINITION)).thenReturn(mockTopology);

    PackingPlan resultPacking = LauncherUtils.getInstance().createPackingPlan(mockConfig,
        mockConfig);
    Assert.assertEquals(mockPackingPlan, resultPacking);
    Mockito.verify(mockPacking).initialize(Mockito.any(Config.class), Mockito.eq(mockTopology));
    Mockito.verify(mockPacking).pack();
    Mockito.verify(mockPacking).close();
  }

  @Test
  public void constructsRuntimeWithPackingProperly() {
    Config runtime = Config.newBuilder().put("key-23", "value-34").build();
    Assert.assertNull(Runtime.componentRamMap(runtime));

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));
    containers.add(Mockito.mock(PackingPlan.ContainerPlan.class));

    PackingPlan mockPacking = Mockito.mock(PackingPlan.class);
    Mockito.when(mockPacking.getComponentRamDistribution()).thenReturn("ramMap");
    Mockito.when(mockPacking.getContainers()).thenReturn(containers);

    Config newRuntime = LauncherUtils.getInstance()
        .createConfigWithPackingDetails(runtime, mockPacking);
    Assert.assertNull(Runtime.componentRamMap(runtime));
    Assert.assertEquals("ramMap", Runtime.componentRamMap(newRuntime));
    Assert.assertEquals(3, Runtime.numContainers(newRuntime).longValue());
    Assert.assertEquals("value-34", newRuntime.getStringValue("key-23"));
  }

  @Test
  public void constructsConfigWithTopologyInfo() throws Exception {
    TopologyAPI.Topology mockTopology = PowerMockito.mock(TopologyAPI.Topology.class);
    PowerMockito.when(mockTopology.getId()).thenReturn("testTopologyId");
    PowerMockito.when(mockTopology.getName()).thenReturn("testTopologyName");

    SchedulerStateManagerAdaptor mockStMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);

    PowerMockito.spy(TopologyUtils.class);
    PowerMockito.doReturn(456).when(TopologyUtils.class, "getNumContainers", mockTopology);

    Config runtime = Config.newBuilder()
        .putAll(LauncherUtils.getInstance().createPrimaryRuntime(mockTopology))
        .putAll(LauncherUtils.getInstance().createAdaptorRuntime(mockStMgr))
            .build();
    Assert.assertEquals("testTopologyId", Runtime.topologyId(runtime));
    Assert.assertEquals("testTopologyName", Runtime.topologyName(runtime));
    Assert.assertEquals(mockTopology, Runtime.topology(runtime));
    Assert.assertEquals(mockStMgr, Runtime.schedulerStateManagerAdaptor(runtime));
    Assert.assertEquals(456 + 1, Runtime.numContainers(runtime).longValue());
  }
}
