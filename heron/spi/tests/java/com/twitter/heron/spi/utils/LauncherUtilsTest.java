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

package com.twitter.heron.spi.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

@RunWith(PowerMockRunner.class)
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

    Config mockConfig = Mockito.mock(Config.class);
    Mockito.when(mockConfig.getStringValue(Keys.packingClass())).thenReturn(PACKING_CLASS);

    PackingPlan resultPacking = LauncherUtils.createPackingPlan(mockConfig, null);
    Assert.assertEquals(mockPackingPlan, resultPacking);

    Mockito.verify(mockPacking).initialize(Mockito.any(Config.class), Mockito.any(Config.class));
    Mockito.verify(mockPacking).pack();
    Mockito.verify(mockPacking).close();
  }

  @Test
  public void constructsRuntimeWithPackingProperly() {
    Config runtime = Config.newBuilder().put("key-23", "value-34").build();
    Assert.assertNull(Runtime.instanceDistribution(runtime));
    Assert.assertNull(Runtime.componentRamMap(runtime));

    Map<String, PackingPlan.ContainerPlan> containerMap = new HashMap<>();
    containerMap.put("1", null);
    containerMap.put("2", null);

    PackingPlan mockPacking = Mockito.mock(PackingPlan.class);
    Mockito.when(mockPacking.getInstanceDistribution()).thenReturn("instances");
    Mockito.when(mockPacking.getComponentRamDistribution()).thenReturn("ramMap");
    Mockito.when(mockPacking.getContainers()).thenReturn(containerMap);

    Config newRuntime = LauncherUtils.createConfigWithPackingDetails(runtime, mockPacking);
    Assert.assertNull(Runtime.instanceDistribution(runtime));
    Assert.assertNull(Runtime.componentRamMap(runtime));
    Assert.assertEquals("instances", Runtime.instanceDistribution(newRuntime));
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

    Config runtime = LauncherUtils.getPrimaryRuntime(mockTopology, mockStMgr);
    Assert.assertEquals("testTopologyId", Runtime.topologyId(runtime));
    Assert.assertEquals("testTopologyName", Runtime.topologyName(runtime));
    Assert.assertEquals(mockTopology, Runtime.topology(runtime));
    Assert.assertEquals(mockStMgr, Runtime.schedulerStateManagerAdaptor(runtime));
    Assert.assertEquals(456 + 1, Runtime.numContainers(runtime).longValue());
  }
}
