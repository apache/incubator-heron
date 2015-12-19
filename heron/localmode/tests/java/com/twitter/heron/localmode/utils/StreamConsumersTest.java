package com.twitter.heron.localmode.utils;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.localmode.grouping.Grouping;
import com.twitter.heron.proto.system.PhysicalPlans;

import junit.framework.Assert;


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
    Map<TopologyAPI.StreamId, StreamConsumers> map = com.twitter.heron.localmode.utils.StreamConsumers.populateStreamConsumers(
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
