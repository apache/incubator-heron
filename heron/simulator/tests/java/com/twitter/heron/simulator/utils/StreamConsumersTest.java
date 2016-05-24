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

package com.twitter.heron.simulator.utils;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.simulator.grouping.Grouping;

/**
 * StreamConsumers Tester.
 */
public class StreamConsumersTest {
  private static PhysicalPlans.PhysicalPlan plan;
  private static TopologyAPI.Topology topology;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = PhysicalPlanUtilTest.getTestTopology();
    plan = PhysicalPlanUtil.getPhysicalPlan(topology);
  }

  @AfterClass
  public static void afterClass() throws Exception {

  }

  @Before
  public void before() throws Exception {

  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: populateStreamConsumers(PhysicalPlans.PhysicalPlan physicalPlan)
   */
  @Test
  public void testPopulateStreamConsumers() throws Exception {
    Map<TopologyAPI.StreamId, StreamConsumers> map =
        com.twitter.heron.simulator.utils.StreamConsumers.populateStreamConsumers(
            topology, PhysicalPlanUtil.getComponentToTaskIds(plan));
    Assert.assertEquals(1, map.size());

    for (Map.Entry<TopologyAPI.StreamId, StreamConsumers> entry : map.entrySet()) {
      TopologyAPI.StreamId streamId = entry.getKey();
      Assert.assertEquals("default", streamId.getId());
      Assert.assertEquals("word", streamId.getComponentName());

      StreamConsumers consumers = entry.getValue();
      Assert.assertNotNull(consumers);
      Assert.assertEquals(1, consumers.getConsumers().size());

      Grouping grouping = consumers.getConsumers().get(0);
      Assert.assertTrue(grouping.getClass().toString().contains("ShuffleGrouping"));

      Assert.assertEquals(1, entry.getValue().getListToSend(null).size());

      List<Integer> boltTasksId = PhysicalPlanUtil.getComponentToTaskIds(plan).get("exclaim");
      Integer targetId = entry.getValue().getListToSend(null).get(0);
      Assert.assertTrue(boltTasksId.contains(targetId));
    }
  }
}
