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
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.Files;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
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
public class SubmitDryRunRenderTest {

  private ContainerPlan containerOnePlan;
  private ContainerPlan containerTwoPlan;
  private PackingPlan plan;

  @Before
  public void setUp() throws Exception {
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
    containerOnePlan = new ContainerPlan(1, instancesOne, requiredResource);
    Set<InstancePlan> instancesTwo = new HashSet<>();
    instancesTwo.add(new InstancePlan(
        new InstanceId("exclaim1", 4, 0), instanceResource));
    instancesTwo.add(new InstancePlan(
        new InstanceId("exclaim1", 2, 0), instanceResource));
    instancesTwo.add(new InstancePlan(
        new InstanceId("word", 6, 0), instanceResource));
    containerTwoPlan = new ContainerPlan(2, instancesTwo, requiredResource);
    Set<ContainerPlan> containerPlans = new HashSet<>();
    containerPlans.add(containerOnePlan);
    containerPlans.add(containerTwoPlan);
    plan = new PackingPlan("A", containerPlans);
  }

  @Test
  public void testTableA() throws IOException {
    String filePath = Paths.get(
        System.getenv("JAVA_RUNFILES"),
        Constants.TEST_DATA_PATH, "SubmitDryRunOutputATable.txt").toString();
    File sampleOutput = new File(filePath);
    TopologyAPI.Topology topology = PowerMockito.mock(TopologyAPI.Topology.class);
    Config config = Config.newBuilder().put(ConfigKeys.get("PACKING_CLASS"),
        "com.twitter.heron.packing.roundrobin.RoundRobinPacking").build();
    String table =
        new SubmitTableDryRunRenderer(new SubmitDryRunResponse(topology, config, plan)).render();
    String exampleTable = Files.toString(sampleOutput, StandardCharsets.UTF_8);
    assertEquals(exampleTable, table);
  }
}
