//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package org.apache.heron.scheduler.dryrun;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.PackingTestUtils;

import static junit.framework.TestCase.assertEquals;
import static org.apache.heron.spi.packing.PackingPlan.ContainerPlan;

public class JsonFormatterUtilsTest {

  private PackingPlan plan;

  @Before
  public void setUp() throws Exception {
    final String COMPONENT_A = "exclaim1";
    final String COMPONENT_B = "word";
    ContainerPlan containerPlanA = PackingTestUtils.testContainerPlan(
        1, new Pair<>(COMPONENT_A, 1),
        new Pair<>(COMPONENT_A, 3),
        new Pair<>(COMPONENT_B, 5));
    ContainerPlan containerPlanB = PackingTestUtils.testContainerPlan(
        2, new Pair<>(COMPONENT_A, 2),
        new Pair<>(COMPONENT_A, 4),
        new Pair<>(COMPONENT_B, 6));
    Set<ContainerPlan> containerPlans = new HashSet<>();
    containerPlans.add(containerPlanA);
    containerPlans.add(containerPlanB);
    plan = new PackingPlan("A", containerPlans);
  }

  @Test
  public void testRenderPackingPlan() throws Exception {
    JsonFormatterUtils utils = new JsonFormatterUtils();
    String packingPlanJson = utils.renderPackingPlan("test-topology",
        "org.apache.heron.packing.roundrobin.RoundRobingPacking", plan);

    String filename =  "/heron/scheduler-core/tests/resources/JsonFormatterUtilsExpectedJson.txt";
    InputStream stream = JsonFormatterUtils.class.getResourceAsStream(filename);
    if (stream == null) {
      throw new RuntimeException("Expected json file cannot be found or opened.");
    }
    // Input might contain UTF-8 character, so we read stream with UTF-8 decoding
    String expectedJson = IOUtils.toString(stream, StandardCharsets.UTF_8);

    assertEquals(expectedJson, packingPlanJson);
  }
}
