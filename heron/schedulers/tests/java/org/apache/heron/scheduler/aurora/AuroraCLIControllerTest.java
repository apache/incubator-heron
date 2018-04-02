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

package org.apache.heron.scheduler.aurora;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.heron.spi.packing.PackingPlan.ContainerPlan;
import org.apache.heron.spi.utils.PackingTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    controller = spy(
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
    doReturn(false).when(controller).runProcess(anyListOf(String.class));
    assertFalse(controller.createJob(bindings));
    verify(controller).runProcess(eq(expectedCommand));

    // Happy path
    doReturn(true).when(controller).runProcess(anyListOf(String.class));
    assertTrue(controller.createJob(bindings));
    verify(controller, times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testKillJob() throws Exception {
    List<String> expectedCommand = asList("aurora job killall %s %s %s %d",
        JOB_SPEC, VERBOSE_CONFIG, BATCH_CONFIG, Integer.MAX_VALUE);

    // Failed
    doReturn(false).when(controller).runProcess(anyListOf(String.class));
    assertFalse(controller.killJob());
    verify(controller).runProcess(eq(expectedCommand));

    // Happy path
    doReturn(true).when(controller).runProcess(anyListOf(String.class));
    assertTrue(controller.killJob());
    verify(controller, times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testRestartJob() throws Exception {
    int containerId = 1;
    List<String> expectedCommand = asList("aurora job restart %s/%s %s %s %d",
        JOB_SPEC, containerId, VERBOSE_CONFIG, BATCH_CONFIG, Integer.MAX_VALUE);

    // Failed
    doReturn(false).when(controller).runProcess(anyListOf(String.class));
    assertFalse(controller.restart(containerId));
    verify(controller).runProcess(eq(expectedCommand));

    // Happy path
    doReturn(true).when(controller).runProcess(anyListOf(String.class));
    assertTrue(controller.restart(containerId));
    verify(controller, times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testRemoveContainers() {
    class ContainerPlanComparator implements Comparator<ContainerPlan> {
      @Override
      public int compare(ContainerPlan o1, ContainerPlan o2) {
        return Integer.compare(o1.getId(), o2.getId());
      }
    }
    SortedSet<ContainerPlan> containers = new TreeSet<>(new ContainerPlanComparator());
    containers.add(PackingTestUtils.testContainerPlan(3));
    containers.add(PackingTestUtils.testContainerPlan(5));

    List<String> expectedCommand = asList("aurora job kill %s/3,5 %s %s %d",
        JOB_SPEC, VERBOSE_CONFIG, BATCH_CONFIG, Integer.MAX_VALUE);

    doReturn(true).when(controller).runProcess(anyListOf(String.class));
    controller.removeContainers(containers);
    verify(controller).runProcess(eq(expectedCommand));
  }

  @Test
  public void testAddContainers() {
    Integer containersToAdd = 3;
    List<String> expectedCommand = asList(
        "aurora job add --wait-until RUNNING %s/0 %s %s",
        JOB_SPEC, containersToAdd.toString(), VERBOSE_CONFIG);

    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock arg0) throws Throwable {
        final StringBuilder originalArgument = (StringBuilder) (arg0.getArguments())[2];
        originalArgument.append("Querying instance statuses: [1, 2, 3]");
        return true;
      }
    }).when(controller).runProcess(
                    anyListOf(String.class),
                    any(StringBuilder.class),
                    any(StringBuilder.class));
    Set<Integer> ret = controller.addContainers(containersToAdd);
    assertEquals(containersToAdd.intValue(), ret.size());
    verify(controller).runProcess(eq(expectedCommand), any(), any());
  }

  @Test
  public void testAddContainersFailure() {
    Integer containersToAdd = 3;
    List<String> expectedCommand = asList(
        "aurora job add --wait-until RUNNING %s/0 %s %s",
        JOB_SPEC, containersToAdd.toString(), VERBOSE_CONFIG);

    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock arg0) throws Throwable {
        final StringBuilder originalArgument = (StringBuilder) (arg0.getArguments())[2];
        originalArgument.append("Querying instance statuses: x");
        return true;
      }
    }).when(controller).runProcess(anyListOf(String.class), any(StringBuilder.class), any(StringBuilder.class));
    Set<Integer> ret = controller.addContainers(containersToAdd);
    assertEquals(0, ret.size());
    verify(controller).runProcess(eq(expectedCommand), any(), any());
  }

  private static List<String> asList(String command, Object... values) {
    return new ArrayList<>(Arrays.asList(String.format(command, values).split(" ")));
  }
}
