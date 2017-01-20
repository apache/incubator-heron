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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.Files;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import org.apache.commons.io.IOUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

import static com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import static com.twitter.heron.spi.packing.PackingPlan.InstancePlan;
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
    Resource instanceResource = new Resource(
        1.0, ByteAmount.fromGigabytes(3), ByteAmount.fromGigabytes(1));
    Resource requiredResource = new Resource(
        5.0, ByteAmount.fromGigabytes(11), ByteAmount.fromGigabytes(5));
    Set<InstancePlan> instancesOne = new HashSet<>();
    instancesOne.add(new InstancePlan(
        new InstanceId("exclaim1", 1, 0), instanceResource));
    instancesOne.add(new InstancePlan(
        new InstanceId("exclaim1", 3, 0), instanceResource));
    instancesOne.add(new InstancePlan(
        new InstanceId("word", 5, 0), instanceResource));
    ContainerPlan containerOnePlan = new ContainerPlan(1, instancesOne, requiredResource);
    Set<InstancePlan> instancesTwo = new HashSet<>();
    instancesTwo.add(new InstancePlan(
        new InstanceId("exclaim1", 4, 0), instanceResource));
    instancesTwo.add(new InstancePlan(
        new InstanceId("exclaim1", 2, 0), instanceResource));
    instancesTwo.add(new InstancePlan(
        new InstanceId("word", 6, 0), instanceResource));
    ContainerPlan containerTwoPlan = new ContainerPlan(2, instancesTwo, requiredResource);
    Set<ContainerPlan> containerPlans = new HashSet<>();
    containerPlans.add(containerOnePlan);
    containerPlans.add(containerTwoPlan);
    originalPlan = new PackingPlan("ORIG", containerPlans);

    // setup new packing plan A: word:1, exclaim1:9
    Set<ContainerPlan> containerPlansA = new HashSet<>();
    Resource planACommonRequiredResource = new Resource(
        3.0, ByteAmount.fromGigabytes(10), ByteAmount.fromGigabytes(3));
    // add container one
    containerPlansA.add(new ContainerPlan(1, instancesOne, planACommonRequiredResource));
    // add container two
    Set<InstancePlan> planAContainerTwoPlans = new HashSet<>();
    planAContainerTwoPlans.add(new InstancePlan(
        new InstanceId("exclaim1", 4, 0), instanceResource));
    planAContainerTwoPlans.add(new InstancePlan(
        new InstanceId("exclaim1", 2, 0), instanceResource));
    planAContainerTwoPlans.add(new InstancePlan(
        new InstanceId("exclaim1", 6, 0), instanceResource));
    containerPlansA.add(new ContainerPlan(2, planAContainerTwoPlans, planACommonRequiredResource));
    // add container three
    Set<InstancePlan> planAContainerThreePlans = new HashSet<>();
    planAContainerThreePlans.add(new InstancePlan(
        new InstanceId("exclaim1", 7, 0), instanceResource));
    planAContainerThreePlans.add(new InstancePlan(
        new InstanceId("exclaim1", 8, 0), instanceResource));
    planAContainerThreePlans.add(new InstancePlan(
        new InstanceId("exclaim1", 9, 0), instanceResource));
    containerPlansA.add(new ContainerPlan(3, planAContainerThreePlans,
        planACommonRequiredResource));
    // add container four
    Set<InstancePlan> planAContainerFourPlans = new HashSet<>();
    planAContainerFourPlans.add(new InstancePlan(
        new InstanceId("exclaim1", 10, 0), instanceResource));
    containerPlansA.add(new ContainerPlan(4, planAContainerFourPlans,
        new Resource(1.0, ByteAmount.fromGigabytes(3), ByteAmount.fromGigabytes(1))));
    newPlanA = new PackingPlan("A", containerPlansA);

    // setup new packing plan B: word:1, exclaim:1
    Set<ContainerPlan> containerPlansB = new HashSet<>();
    Resource containerResource = new Resource(
        2.0, ByteAmount.fromGigabytes(7), ByteAmount.fromGigabytes(2));
    Set<InstancePlan> planBContainerOnePlans = new HashSet<>();
    planBContainerOnePlans.add(new InstancePlan(
        new InstanceId("exclaim1", 3, 0), instanceResource));
    planBContainerOnePlans.add(new InstancePlan(
        new InstanceId("word", 5, 0), instanceResource));
    containerPlansB.add(
        new ContainerPlan(1, planBContainerOnePlans, containerResource));
    newPlanB = new PackingPlan("B", containerPlansB);
  }

  @Test
  public void testTableA() throws IOException {
    InputStream stream  = UpdateDryRunRenderTest.class.
        getResourceAsStream("/heron/scheduler-core/tests/resources/UpdateDryRunOutputATable.txt");
    if (stream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    String exampleTable = IOUtils.toString(stream);
    TopologyAPI.Topology topology = PowerMockito.mock(TopologyAPI.Topology.class);
    Config config = Config.newBuilder().put(ConfigKeys.get("REPACKING_CLASS"),
        "com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking").build();
    UpdateDryRunResponse resp = new UpdateDryRunResponse(
        topology, config, newPlanA, originalPlan, new HashMap<String, Integer>());
    String table =
        new UpdateTableDryRunRenderer(resp).render();
    assertEquals(exampleTable, table);
  }

  @Test
  public void testTableB() throws IOException {
    InputStream stream  = UpdateDryRunRenderTest.class.
        getResourceAsStream("/heron/scheduler-core/tests/resources/UpdateDryRunOutputBTable.txt");
    if (stream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    String exampleTable = IOUtils.toString(stream);
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
