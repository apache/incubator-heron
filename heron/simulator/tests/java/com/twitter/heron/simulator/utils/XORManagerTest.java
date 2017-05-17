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

import java.time.Duration;
import java.util.HashMap;
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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.WakeableLooper;

/**
 * XORManager Tester.
 */
public class XORManagerTest {

  private static List<Integer> taskIds = new LinkedList<>();
  private static TopologyAPI.Topology topology;
  private static Duration timeout = Duration.ofSeconds(1);
  private static int nBuckets = 3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    topology = PhysicalPlanUtilTest.getTestTopology();
    taskIds.add(1);
    taskIds.add(2);
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

    XORManager g = new XORManager(looper, timeout, taskIds, nBuckets);

    Mockito.verify(looper).registerTimerEvent(Mockito.eq(timeout), Mockito.any(Runnable.class));

    // Create some items
    for (int i = 0; i < 100; ++i) {
      g.create(1, i, 1);
    }

    // basic Anchor works
    for (int i = 0; i < 100; ++i) {
      Assert.assertEquals(g.anchor(1, i, 1), true);
      Assert.assertEquals(g.remove(1, i), true);
    }

    // layered anchoring
    List<Long> thingsAdded = new LinkedList<>();
    Random random = new Random();
    Long firstKey = random.nextLong();
    g.create(1, 1, firstKey);
    thingsAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      thingsAdded.add(key);
      Assert.assertEquals(g.anchor(1, 1, key), false);
    }

    // xor ing works
    for (int j = 0; j < 99; ++j) {
      Assert.assertEquals(g.anchor(1, 1, thingsAdded.get(j)), false);
    }

    Assert.assertEquals(g.anchor(1, 1, thingsAdded.get(99)), true);
    Assert.assertEquals(g.remove(1, 1), true);

    // Same test with some rotation
    List<Long> oneAdded = new LinkedList<>();
    firstKey = random.nextLong();
    g.create(1, 1, firstKey);
    oneAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      oneAdded.add(key);
      Assert.assertEquals(g.anchor(1, 1, key), false);
    }

    g.rotate();
    Mockito.verify(looper).registerTimerEvent(Mockito.eq(rotateInterval),
        Mockito.any(Runnable.class));
    for (int j = 0; j < 99; ++j) {
      Assert.assertEquals(g.anchor(1, 1, oneAdded.get(j)), false);
    }

    Assert.assertEquals(g.anchor(1, 1, oneAdded.get(99)), true);
    Assert.assertEquals(g.remove(1, 1), true);


    // Same test with too much rotation
    List<Long> twoAdded = new LinkedList<>();
    firstKey = random.nextLong();
    g.create(2, 1, firstKey);
    twoAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = random.nextLong();
      twoAdded.add(key);
      Assert.assertEquals(g.anchor(2, 1, key), false);
    }

    // We do #nBuckets rotate()
    for (int i = 0; i < nBuckets; i++) {
      g.rotate();
    }
    // We expected (nBuckets+1) since we have done one rotate earlier
    Mockito.verify(looper, Mockito.times(nBuckets + 1)).registerTimerEvent(
        Mockito.eq(rotateInterval), Mockito.any(Runnable.class));

    for (int j = 0; j < 100; ++j) {
      Assert.assertEquals(g.anchor(2, 1, twoAdded.get(j)), false);
    }

    Assert.assertEquals(g.remove(2, 1), false);
  }

  /**
   * Method: populateXORManager(WakeableLooper looper, TopologyAPI.Topology topology, int nBuckets, Map&lt;String, List&lt;Integer&gt;&gt; componentToTaskIds)
   */
  @Test
  public void testPopulateXORManager() throws Exception {
    Map<String, List<Integer>> componentToTaskIds =
        new HashMap<>();
    WakeableLooper looper = Mockito.mock(WakeableLooper.class);

    componentToTaskIds.put("word", taskIds);
    XORManager manager = XORManager.populateXORManager(looper,
        topology,
        3,
        componentToTaskIds);

    Map<Integer, RotatingMap> spoutTasksToRotatingMap = manager.getSpoutTasksToRotatingMap();
    Assert.assertEquals(taskIds.size(), spoutTasksToRotatingMap.size());
    Assert.assertEquals(new HashSet<>(taskIds), spoutTasksToRotatingMap.keySet());
  }
}

