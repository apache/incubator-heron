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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class AuroraControllerTest {
  private static final String JOB_NAME = "jobName";
  private static final String CLUSTER = "cluster";
  private static final String ROLE = "role";
  private static final String ENV = "gz";
  private static final String VERBOSE_CONFIG = "--verbose";
  private static final String BATCH_CONFIG = "--batch-size";
  private static final boolean IS_VERBOSE = true;

  private AuroraController controller;

  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    controller = Mockito.spy(new AuroraController(JOB_NAME, CLUSTER, ROLE, ENV, IS_VERBOSE));
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testCreateJob() throws Exception {
    String auroraFilename = "file.aurora";
    Map<String, String> bindings = new HashMap<>();
    List<String> expectedCommand =
        new ArrayList<>(Arrays.asList("aurora", "job", "create", "--wait-until", "RUNNING",
            String.format("%s/%s/%s/%s", CLUSTER, ROLE, ENV, JOB_NAME),
            auroraFilename, VERBOSE_CONFIG));

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertFalse(controller.createJob(auroraFilename, bindings));
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertTrue(controller.createJob(auroraFilename, bindings));
    Mockito.verify(controller, Mockito.times(2)).runProcess(expectedCommand);
  }

  @Test
  public void testKillJob() throws Exception {
    List<String> expectedCommand =
        new ArrayList<>(Arrays.asList("aurora", "job", "killall",
            String.format("%s/%s/%s/%s", CLUSTER, ROLE, ENV, JOB_NAME),
            VERBOSE_CONFIG, BATCH_CONFIG, Integer.toString(Integer.MAX_VALUE)));

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
    List<String> expectedCommand =
        new ArrayList<>(Arrays.asList("aurora", "job", "restart",
            String.format("%s/%s/%s/%s/%d", CLUSTER, ROLE, ENV, JOB_NAME, containerId),
            VERBOSE_CONFIG, BATCH_CONFIG, Integer.toString(Integer.MAX_VALUE)));

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertFalse(controller.restartJob(containerId));
    Mockito.verify(controller).runProcess(Mockito.eq(expectedCommand));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyListOf(String.class));
    Assert.assertTrue(controller.restartJob(containerId));
    Mockito.verify(controller, Mockito.times(2)).runProcess(expectedCommand);
  }
}
