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
package com.twitter.heron.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.packing.PackingPlan;

public class UpdateTopologyManagerTest {
  @Test
  public void findsNewContainersInUpdatedPacking() {
    PackingPlan.ContainerPlan mockContainerPlan = Mockito.mock(PackingPlan.ContainerPlan.class);

    Map<String, PackingPlan.ContainerPlan> currentPlan = new HashMap<>();
    currentPlan.put("current-1", mockContainerPlan);
    currentPlan.put("current-2", mockContainerPlan);
    currentPlan.put("current-3", mockContainerPlan);

    Map<String, PackingPlan.ContainerPlan> updatedPlan = new HashMap<>();
    updatedPlan.put("current-1", mockContainerPlan);
    updatedPlan.put("current-2", mockContainerPlan);
    updatedPlan.put("new-1", mockContainerPlan);
    updatedPlan.put("new-2", mockContainerPlan);

    UpdateTopologyManager manager = new UpdateTopologyManager(null, null);
    Map<String, PackingPlan.ContainerPlan> result;
    result = manager.getNewContainers(currentPlan, updatedPlan);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.containsKey("new-1"));
    Assert.assertTrue(result.containsKey("new-2"));
  }
}
