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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * RotatingMap Tester.
 */
public class RotatingMapTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * // Test the remove and rotate
   */
  @Test
  public void testRemoveAndRotate() throws Exception {
    int nBuckets = 3;
    RotatingMap g = new RotatingMap(nBuckets);

    // Create some items
    for (int i = 0; i < 100; i++) {
      g.create(i, 1);
    }

    // Make sure that removes work
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(g.remove(i));
    }

    // Unknown items cant be removed
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse(g.remove(i));
    }

    // Create some more
    for (int i = 0; i < 100; ++i) {
      g.create(i, 1);
    }

    // Rotate once
    g.rotate();

    // Make sure that removes work
    for (int i = 0; i < 100; ++i) {
      Assert.assertTrue(g.remove(i));
    }

    // Unknown items cant be removed
    for (int i = 0; i < 100; ++i) {
      Assert.assertFalse(g.remove(i));
    }

    // Create some more
    for (int i = 0; i < 100; ++i) {
      g.create(i, 1);
    }

    // Rotate nBuckets times
    for (int i = 0; i < nBuckets; ++i) {
      g.rotate();
    }

    // removes dont work
    for (int i = 0; i < 100; ++i) {
      Assert.assertFalse(g.remove(i));
    }
  }

  /**
   * Test the anchor logic
   */
  @Test
  public void testAnchor() throws Exception {
    int nBuckets = 3;
    RotatingMap g = new RotatingMap(nBuckets);

    // Create some items
    for (int i = 0; i < 100; ++i) {
      g.create(i, 1);
    }

    // basic Anchor works
    for (int i = 0; i < 100; ++i) {
      Assert.assertEquals(g.anchor(i, 1), true);
      Assert.assertEquals(g.remove(i), true);
    }

    // layered anchoring
    List<Long> thingsAdded = new ArrayList<>();
    long firstKey = new Random().nextLong();
    g.create(1, firstKey);
    thingsAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = new Random().nextLong();
      thingsAdded.add(key);
      Assert.assertEquals(g.anchor(1, key), false);
    }

    // xor ing works
    for (int j = 0; j < 99; ++j) {
      Assert.assertEquals(g.anchor(1, thingsAdded.get(j)), false);
    }

    Assert.assertEquals(g.anchor(1, thingsAdded.get(99)), true);
    Assert.assertEquals(g.remove(1), true);

    // Same test with some rotation
    thingsAdded.clear();
    firstKey = new Random().nextLong();
    g.create(1, firstKey);
    thingsAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = new Random().nextLong();
      thingsAdded.add(key);
      Assert.assertEquals(g.anchor(1, key), false);
    }

    g.rotate();

    for (int j = 0; j < 99; ++j) {
      Assert.assertEquals(g.anchor(1, thingsAdded.get(j)), false);
    }

    Assert.assertEquals(g.anchor(1, thingsAdded.get(99)), true);
    Assert.assertEquals(g.remove(1), true);

    // Too much rotation
    thingsAdded.clear();
    firstKey = new Random().nextLong();
    g.create(1, firstKey);
    thingsAdded.add(firstKey);
    for (int j = 1; j < 100; ++j) {
      long key = new Random().nextLong();
      thingsAdded.add(key);
      Assert.assertEquals(g.anchor(1, key), false);
    }

    for (int i = 0; i < nBuckets; ++i) {
      g.rotate();
    }

    for (int j = 0; j < 100; ++j) {
      Assert.assertEquals(g.anchor(1, thingsAdded.get(j)), false);
    }

    Assert.assertEquals(g.remove(1), false);
  }
}
