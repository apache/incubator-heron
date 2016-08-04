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

package com.twitter.heron.spi.packing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.common.Constants;

public class PackingPlanTest {
  public static PackingPlan generatePacking(Map<String, List<String>> basePacking) {
    Resource resource =
        new Resource(1.0, 1 * Constants.GB, 10 * Constants.GB);

    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();

    for (Map.Entry<String, List<String>> entry : basePacking.entrySet()) {
      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();

      for (String instanceId : instanceList) {
        String componentName = instanceId.split(":")[1];
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(instanceId, componentName, resource);
        instancePlanMap.put(instanceId, instancePlan);
      }

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
    }

    return new PackingPlan("", containerPlanMap, resource);
  }

  @Test
  public void testPackingToString() {
    Map<String, List<String>> packing = new HashMap<>();
    packing.put("1", Arrays.asList("1:spout:1:0", "1:bolt:3:0"));
    String packingStr = generatePacking(packing).getInstanceDistribution();
    String expectedStr0 = "1:spout:1:0:bolt:3:0";
    String expectedStr1 = "1:bolt:3:0:spout:1:0";

    Assert.assertTrue(packingStr.equals(expectedStr0) || packingStr.equals(expectedStr1));

    packing.put("2", Arrays.asList("2:spout:2:1"));
    packingStr = generatePacking(packing).getInstanceDistribution();

    for (String str : packingStr.split(",")) {
      if (str.startsWith("1:")) {
        // This is the packing str for container 1
        Assert.assertTrue(str.equals(expectedStr0) || str.equals(expectedStr1));
      } else if (str.startsWith("2:")) {
        // This is the packing str for container 2
        Assert.assertEquals("2:spout:2:1", str);
      } else {
        // Unexpected container string
        throw new RuntimeException("Unexpected results");
      }
    }
  }
}
