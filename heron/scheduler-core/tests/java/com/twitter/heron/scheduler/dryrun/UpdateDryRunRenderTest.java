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
package com.twitter.heron.scheduler.dryrun;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.PackingTestUtils;

import static com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TopologyAPI.Topology.class)
public class UpdateDryRunRenderTest {

  private PackingPlan originalPlan;
  private PackingPlan newPlanA;
  private PackingPlan newPlanB;

  @Before
  public void setUp() throws Exception {
    // set up original packing plan
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
    originalPlan = new PackingPlan("ORIG", containerPlans);

    // setup new packing plan A: word:1, exclaim1:9
    Set<ContainerPlan> containerPlansA = new HashSet<>();
    containerPlansA.add(containerPlanA);
    containerPlansA.add(PackingTestUtils.testContainerPlan(
        2, new Pair<>(COMPONENT_A, 4),
           new Pair<>(COMPONENT_A, 2),
           new Pair<>(COMPONENT_A, 6)));
    containerPlansA.add(PackingTestUtils.testContainerPlan(
        3, new Pair<>(COMPONENT_A, 7),
           new Pair<>(COMPONENT_A, 8),
           new Pair<>(COMPONENT_A, 9)));
    containerPlansA.add(PackingTestUtils.testContainerPlan(
        4, new Pair<>(COMPONENT_A, 10)));
    newPlanA = new PackingPlan("A", containerPlansA);

    // setup new packing plan B: word:1, exclaim1:1
    Set<ContainerPlan> containerPlansB = new HashSet<>();
    containerPlansB.add(PackingTestUtils.testContainerPlan(
        1, new Pair<>(COMPONENT_A, 3),
           new Pair<>(COMPONENT_B, 5)));
    newPlanB = new PackingPlan("B", containerPlansB);
  }

  @Test
  public void testTableA() throws IOException {
    InputStream stream  = UpdateDryRunRenderTest.class.
        getResourceAsStream("/heron/scheduler-core/tests/resources/UpdateDryRunOutputATable.txt");
    if (stream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    // Input might contain UTF-8 character, so we read stream with UTF-8 decoding
    String exampleTable = IOUtils.toString(stream, StandardCharsets.UTF_8);
    TopologyAPI.Topology topology = PowerMockito.mock(TopologyAPI.Topology.class);
    Config config = Config.newBuilder().put(ConfigKeys.get("REPACKING_CLASS"),
        "com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking").build();
    UpdateDryRunResponse resp = new UpdateDryRunResponse(
        topology, config, newPlanA, originalPlan, new HashMap<String, Integer>());
    String table =
        new UpdateTableDryRunRenderer(resp).render();
    assertEquals(exampleTable, table);
  }

  @Test public void testTableB() throws IOException {
    InputStream stream  = UpdateDryRunRenderTest.class.
        getResourceAsStream("/heron/scheduler-core/tests/resources/UpdateDryRunOutputBTable.txt");
    if (stream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    // Input might contain UTF-8 character, so we read stream with UTF-8 decoding
    String exampleTable = IOUtils.toString(stream, StandardCharsets.UTF_8);
    TopologyAPI.Topology topology = PowerMockito.mock(TopologyAPI.Topology.class);
    Config config = Config.newBuilder().put(ConfigKeys.get("REPACKING_CLASS"),
        "com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking").build();
    UpdateDryRunResponse resp = new UpdateDryRunResponse(
        topology, config, newPlanB, originalPlan, new HashMap<String, Integer>());
    String table =
        new UpdateTableDryRunRenderer(resp).render();
    assertEquals(exampleTable, table);
  }
}
