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

package com.twitter.heron.scheduler.aurora;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.PackingTestUtils;

public class AuroraCLIControllerTest {
  private static final String JOB_NAME = "jobName";
  private static final String CLUSTER = "cluster";
  private static final String ROLE = "role";
  private static final String ENV = "gz";
  private static final String AURORA_FILENAME = "file.aurora";
  private static final String VERBOSE_CONFIG = "--verbose";
  private static final String BATCH_CONFIG = "--batch-size";
  private static final String JOB_SPEC = String.format("%s/%s/%s/%s", CLUSTER, ROLE, ENV, JOB_NAME);
  private static final boolean IS_VERBOSE = true;

  private AuroraCLIController controller;

  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    controller = Mockito.spy(
        new AuroraCLIController(JOB_NAME, CLUSTER, ROLE, ENV, AURORA_FILENAME, IS_VERBOSE));
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testCreateJob() throws Exception {
    Map<AuroraField, String> bindings = new HashMap<>();
    List<String> expectedCommand = asList("aurora job create --wait-until RUNNING %s %s %s",
            JOB_SPEC, AURORA_FILENAME, VERBOSE_CONFIG);

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertFalse(controller.createJob(bindings));
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertTrue(controller.createJob(bindings));
    Mockito.verify(controller, Mockito.times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testKillJob() throws Exception {
    List<String> expectedCommand = asList("aurora job killall %s %s %s %d",
        JOB_SPEC, VERBOSE_CONFIG, BATCH_CONFIG, Integer.MAX_VALUE);

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertFalse(controller.killJob());
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertTrue(controller.killJob());
    Mockito.verify(controller, Mockito.times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testRestartJob() throws Exception {
    int containerId = 1;
    List<String> expectedCommand = asList("aurora job restart %s/%s %s %s %d",
        JOB_SPEC, containerId, VERBOSE_CONFIG, BATCH_CONFIG, Integer.MAX_VALUE);

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertFalse(controller.restart(containerId));
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertTrue(controller.restart(containerId));
    Mockito.verify(controller, Mockito.times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testRemoveContainers() {
    class ContainerPlanComparator implements Comparator<PackingPlan.ContainerPlan> {
      @Override
      public int compare(PackingPlan.ContainerPlan o1, PackingPlan.ContainerPlan o2) {
        return ((Integer) o1.getId()).compareTo(o2.getId());
      }
    }
    SortedSet<PackingPlan.ContainerPlan> containers = new TreeSet<>(new ContainerPlanComparator());
    containers.add(PackingTestUtils.testContainerPlan(3));
    containers.add(PackingTestUtils.testContainerPlan(5));

    List<String> expectedCommand = asList("aurora job kill %s/3,5", JOB_SPEC);

    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    controller.removeContainers(containers);
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));
  }

  @Test
  public void testAddContainers() {
    Integer containersToAdd = 3;
    List<String> expectedCommand = asList(
        "aurora job add --wait-until RUNNING %s/0 %s", JOB_SPEC, containersToAdd.toString());

    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    controller.addContainers(containersToAdd);
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));
  }

  private static List<String> asList(String command, Object... values) {
    return new ArrayList<>(Arrays.asList(String.format(command, values).split(" ")));
  }
}
