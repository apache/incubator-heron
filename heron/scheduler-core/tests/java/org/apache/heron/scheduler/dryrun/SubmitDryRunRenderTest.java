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

package org.apache.heron.scheduler.dryrun;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.commons.io.IOUtils;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.PackingTestUtils;

import static org.apache.heron.spi.packing.PackingPlan.ContainerPlan;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(TopologyAPI.Topology.class)
public class SubmitDryRunRenderTest {

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

  private void test(String filename, boolean rich) throws IOException {
    InputStream stream  = SubmitDryRunRenderTest.class.
        getResourceAsStream(filename);
    if (stream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    // Input might contain UTF-8 character, so we read stream with UTF-8 decoding
    String exampleTable = IOUtils.toString(stream, StandardCharsets.UTF_8);
    TopologyAPI.Topology topology = PowerMockito.mock(TopologyAPI.Topology.class);
    Config config = Config.newBuilder().put(Key.PACKING_CLASS,
        "org.apache.heron.packing.roundrobin.RoundRobinPacking").build();
    String table =
        new SubmitTableDryRunRenderer(
            new SubmitDryRunResponse(topology, config, plan), rich).render();
    assertEquals(exampleTable, table);
  }

  @Test
  public void testTableA() throws IOException {
    test("/heron/scheduler-core/tests/resources/SubmitDryRunOutputATable.txt", true);
  }

  @Test
  public void testTableANonRich() throws IOException {
    test("/heron/scheduler-core/tests/resources/SubmitDryRunOutputATableNonRich.txt", false);
  }
}
