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

package org.apache.heron.simulator.executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.utils.metrics.MetricsCollector;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.IInstance;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.simulator.utils.TopologyManager;
import org.apache.heron.simulator.utils.TopologyManagerTest;


/**
 * InstanceExecutor Tester.
 */
public class InstanceExecutorTest {
  private static PhysicalPlans.PhysicalPlan plan;
  private static TopologyAPI.Topology topology;
  private static String instanceId;
  private static InstanceExecutor instanceExecutor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = TopologyManagerTest.getTestTopology();
    plan = new TopologyManager(topology).getPhysicalPlan();
    instanceId = plan.getInstances(0).getInstanceId();
    instanceExecutor = Mockito.spy(new InstanceExecutor(plan, instanceId));
    Mockito.doReturn(Mockito.mock(IInstance.class)).when(instanceExecutor).createInstance();
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
   * Method: getStreamInQueue()
   */
  @Test
  public void testGetStreamInQueue() throws Exception {
    Assert.assertNotNull(instanceExecutor.getStreamInQueue());
    Assert.assertEquals(0, instanceExecutor.getStreamInQueue().size());
  }

  /**
   * Method: getStreamOutQueue()
   */
  @Test
  public void testGetStreamOutQueue() throws Exception {
    Assert.assertNotNull(instanceExecutor.getStreamOutQueue());
    Assert.assertEquals(0, instanceExecutor.getStreamOutQueue().size());
  }

  /**
   * Method: getMetricsOutQueue()
   */
  @Test
  public void testGetMetricsOutQueue() throws Exception {
    Assert.assertNotNull(instanceExecutor.getMetricsOutQueue());
    Assert.assertEquals(0, instanceExecutor.getMetricsOutQueue().size());
  }

  /**
   * Method: getInstanceId()
   */
  @Test
  public void testGetInstanceId() throws Exception {
    Assert.assertEquals(instanceId, instanceExecutor.getInstanceId());
  }

  /**
   * Method: getTaskId()
   */
  @Test
  public void testGetTaskId() throws Exception {
    Assert.assertEquals(plan.getInstances(0).getInfo().getTaskId(), instanceExecutor.getTaskId());
  }

  /**
   * Method: createInstance()
   */
  @Test
  public void testCreateInstance() throws Exception {
    Assert.assertNotNull(instanceExecutor.createInstance());
  }

  /**
   * Method: createPhysicalPlanHelper(PhysicalPlans.PhysicalPlan physicalPlan, String instanceId, MetricsCollector metricsCollector)
   */
  @Test
  public void testCreatePhysicalPlanHelper() throws Exception {
    PhysicalPlanHelper physicalPlanHelper =
        instanceExecutor.createPhysicalPlanHelper(plan, instanceId,
            Mockito.mock(MetricsCollector.class));

    Assert.assertNotNull(physicalPlanHelper.getTopologyContext());
  }
}
