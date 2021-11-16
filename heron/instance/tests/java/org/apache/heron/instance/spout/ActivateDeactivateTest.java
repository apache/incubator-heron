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

package org.apache.heron.instance.spout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.ExecutorTester;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.resource.Constants;
import org.apache.heron.resource.UnitTestHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ActivateDeactivateTest {
  private static final String SPOUT_INSTANCE_ID = "spout-id";
  private ExecutorTester executorTester;

  @Before
  public void before() {
    executorTester = new ExecutorTester();
    executorTester.start();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    executorTester.stop();
  }

  /**
   * We will test whether spout would pull activate/deactivate state change and
   * invoke activate()/deactivate()
   */
  @Test
  public void testActivateAndDeactivate() throws Exception {
    CountDownLatch activateLatch = new CountDownLatch(1);
    CountDownLatch deactivateLatch = new CountDownLatch(1);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACTIVATE_COUNT_LATCH, activateLatch);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.DEACTIVATE_COUNT_LATCH, deactivateLatch);

    executorTester.getInControlQueue().offer(buildMessage(TopologyAPI.TopologyState.RUNNING));

    // Now the activateLatch and deactivateLatch should be 1
    assertEquals(1, activateLatch.getCount());
    assertEquals(1, deactivateLatch.getCount());

    // And we start the test
    executorTester.getInControlQueue().offer(buildMessage(TopologyAPI.TopologyState.PAUSED));
    assertTrue(deactivateLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));

    assertEquals(1, activateLatch.getCount());
    assertEquals(0, deactivateLatch.getCount());

    executorTester.getInControlQueue().offer(buildMessage(TopologyAPI.TopologyState.RUNNING));
    assertTrue(activateLatch.await(Constants.TEST_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS));

    assertEquals(0, activateLatch.getCount());
    assertEquals(0, deactivateLatch.getCount());
  }

  private InstanceControlMsg buildMessage(TopologyAPI.TopologyState state) {
    PhysicalPlans.PhysicalPlan physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, state);
    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    return InstanceControlMsg.newBuilder()
        .setNewPhysicalPlanHelper(physicalPlanHelper)
        .build();
  }
}
