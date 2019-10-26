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

package org.apache.heron.packing.builder;

import org.junit.Before;
import org.junit.Test;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.packing.exceptions.ResourceExceededException;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests scorers. Testing multiple scorers from a single test since the tests are so simple.
 */
public class ScorerTest {
  private Container[] testContainers;

  @Before
  public void init() throws ResourceExceededException {
    Resource containerCapacity
        = new Resource(1000, ByteAmount.fromGigabytes(100), ByteAmount.fromGigabytes(100));
    testContainers = new Container[] {
        new Container(1, containerCapacity, Resource.EMPTY_RESOURCE),
        new Container(3, containerCapacity, Resource.EMPTY_RESOURCE),
        new Container(4, containerCapacity, Resource.EMPTY_RESOURCE),
    };

    addInstance(testContainers[0], "A", 0);
    addInstance(testContainers[0], "A", 1);
    addInstance(testContainers[0], "B", 2);
    addInstance(testContainers[0], "B", 3);
    addInstance(testContainers[1], "A", 4);
    addInstance(testContainers[1], "B", 5);
    addInstance(testContainers[1], "B", 6);
    addInstance(testContainers[2], "A", 7);
    addInstance(testContainers[2], "A", 8);
  }

  @Test
  public void testHomogeneityScorer() {
    assertScores(
        new double[] {.5, 1.0 / 3, 1}, false, new HomogeneityScorer("A", false), testContainers);
    assertScores(
        new double[] {.5, 2.0 / 3, 0}, false, new HomogeneityScorer("B", false), testContainers);
  }

  @Test
  public void testHomogeneityScorerBinary() {
    assertScores(new double[] {0, 0, 1}, false, new HomogeneityScorer("A", true), testContainers);
  }

  @Test
  public void testContainerIdScorer() {
    assertScores(new double[] {1, 3, 4}, true, new ContainerIdScorer(), testContainers);
    assertScores(new double[] {1, 3, 4}, false, new ContainerIdScorer(false), testContainers);
  }

  @Test
  public void testContainerIdScorerFromId() {
    assertScores(new double[] {5, 0, 1}, true, new ContainerIdScorer(3, 4), testContainers);
  }

  @Test
  public void testInstanceCountScorer() {
    assertScores(new double[] {4, 3, 2}, true, new InstanceCountScorer(), testContainers);
  }

  private static void addInstance(Container container,
                                  String componentName,
                                  int taskId) throws ResourceExceededException {
    container.add(PackingTestUtils.testInstancePlan(componentName, taskId));
  }

  private static void assertScores(double[] expectedScores, boolean expectedAscending,
                                   Scorer<Container> scorer, Container... containers) {
    double[] results = new double[containers.length];
    int i = 0;
    for (Container container: containers) {
      results[i++] = scorer.getScore(container);
    }
    assertArrayEquals(expectedScores, results, 0.001);
    assertEquals(expectedAscending, scorer.sortAscending());
  }
}
