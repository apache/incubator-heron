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

package com.twitter.heron.healthmgr.common;

import java.util.HashSet;

import org.junit.Test;

import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertEquals;

public class PackingPlanProviderTest {
  @Test
  public void providesBoltInstanceNames() {
    PackingPlan testPacking =
        PackingTestUtils.testPackingPlan("testTopology", new RoundRobinPacking());

    PackingPlanProvider packingPlanProvider = new PackingPlanProvider();
    packingPlanProvider.setPackingPlan(testPacking);

    String[] boltNames = packingPlanProvider.getBoltInstanceNames("testBolt");
    assertEquals(3, boltNames.length);

    HashSet<String> expectedBoltNames = new HashSet<>();
    expectedBoltNames.add("container_1_testBolt_3");
    expectedBoltNames.add("container_1_testBolt_4");
    expectedBoltNames.add("container_1_testBolt_5");

    for (String name : boltNames) {
      expectedBoltNames.remove(name);
    }
    assertEquals(0, expectedBoltNames.size());
  }
}
