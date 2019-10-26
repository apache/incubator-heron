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

package org.apache.heron.simulator.utils;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.WakeableLooper;

/**
 * XORManager Tester.
 */
public class XORManagerTest {

  private static List<Integer> taskIds = new LinkedList<>();
  private static TopologyAPI.Topology topology;
  private static TopologyManager topologyManager;
  private static Duration timeout = Duration.ofSeconds(1);
  private static int nBuckets = 3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = TopologyManagerTest.getTestTopology();
    topologyManager = new TopologyManager(topology);
    taskIds = topologyManager.getComponentToTaskIds().get(TopologyManagerTest.STREAM_ID);
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
   * test basic XORManager methods
   */
  @Test
  public void testXORManagerMethods() throws Exception {
    Duration rotateInterval = timeout.dividedBy(nBuckets).plusNanos(timeout.getNano() % nBuckets);

    WakeableLooper looper = Mockito.mock(WakeableLooper.class);

    XORManager g = new XORManager(looper, topologyManager, nBuckets);

    Mockito.verify(looper).registerTimerEvent(Mockito.eq(timeout), Mockito.any(Runnable.class));

    Integer targetTaskId1 = taskIds.get(0);
    Integer targetTaskId2 = taskIds.get(1);

    // Create some items
    for (int i = 0; i < 100; ++i) {
      g.create(targetTaskId1, i, 1);
    }

    // basic Anchor works
    for (int i = 0; i < 100; ++i) {
      Assert.assertTrue(g.anchor(targetTaskId1, i, 1));
      Assert.assertTrue(g.remove(targetTaskId1, i));
    }

    // layered anchoring
    List<Long> thingsAdded = new LinkedList<>();
    Random random = new Random();
    Long firstKey = random.nextLong();
    g.create(targetTaskId1, 1, firstKey);
    thingsAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      thingsAdded.add(key);
      Assert.assertFalse(g.anchor(targetTaskId1, 1, key));
    }

    // xor ing works
    for (int j = 0; j < 99; ++j) {
      Assert.assertFalse(g.anchor(targetTaskId1, 1, thingsAdded.get(j)));
    }

    Assert.assertTrue(g.anchor(targetTaskId1, 1, thingsAdded.get(99)));
    Assert.assertTrue(g.remove(targetTaskId1, 1));

    // Same test with some rotation
    List<Long> oneAdded = new LinkedList<>();
    firstKey = random.nextLong();
    g.create(targetTaskId1, 1, firstKey);
    oneAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      oneAdded.add(key);
      Assert.assertFalse(g.anchor(targetTaskId1, 1, key));
    }

    g.rotate();
    Mockito.verify(looper).registerTimerEvent(Mockito.eq(rotateInterval),
        Mockito.any(Runnable.class));
    for (int j = 0; j < 99; ++j) {
      Assert.assertFalse(g.anchor(targetTaskId1, 1, oneAdded.get(j)));
    }

    Assert.assertTrue(g.anchor(targetTaskId1, 1, oneAdded.get(99)));
    Assert.assertTrue(g.remove(targetTaskId1, 1));


    // Same test with too much rotation
    List<Long> twoAdded = new LinkedList<>();
    firstKey = random.nextLong();
    g.create(targetTaskId2, 1, firstKey);
    twoAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      twoAdded.add(key);
      Assert.assertFalse(g.anchor(targetTaskId2, 1, key));
    }

    // We do #nBuckets rotate()
    for (int i = 0; i < nBuckets; i++) {
      g.rotate();
    }
    // We expected (nBuckets+1) since we have done one rotate earlier
    Mockito.verify(looper, Mockito.times(nBuckets + 1)).registerTimerEvent(
        Mockito.eq(rotateInterval), Mockito.any(Runnable.class));

    for (int j = 0; j < 100; ++j) {
      Assert.assertFalse(g.anchor(targetTaskId2, 1, twoAdded.get(j)));
    }

    Assert.assertFalse(g.remove(targetTaskId2, 1));
  }

  /**
   * Method: XORManager(WakeableLooper looper, TopologyManager topologyManager, int nBuckets)
   */
  @Test
  public void testXORManager() throws Exception {
    WakeableLooper looper = Mockito.mock(WakeableLooper.class);

    XORManager manager = new XORManager(looper, topologyManager, 3);

    Map<Integer, RotatingMap> spoutTasksToRotatingMap = manager.getSpoutTasksToRotatingMap();
    Assert.assertEquals(taskIds.size(), spoutTasksToRotatingMap.size());
    Assert.assertEquals(new HashSet<>(taskIds), spoutTasksToRotatingMap.keySet());
  }
}

