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

package org.apache.heron.scheduler.slurm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.spi.utils.ShellUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({SlurmContext.class, ShellUtils.class})
public class SlurmControllerTest {
  private static final String WORKING_DIRECTORY = "workingDirectory";
  private static final boolean IS_VERBOSE = true;

  private SlurmController controller;

  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    controller = Mockito.spy(new SlurmController(IS_VERBOSE));
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testCreateJob() throws Exception {
    String slurmFileName = "file.slurm";
    String[] expectedCommand = new String[]{"sbatch", "-N", "1", "--ntasks=1", "slurm", "heron"};

    // Failed
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyString(),
        Matchers.any(String[].class), Matchers.any(StringBuilder.class));
    Assert.assertFalse(controller.createJob(slurmFileName, "slurm",
        expectedCommand, WORKING_DIRECTORY, 1));

    // Happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyString(),
        Matchers.any(String[].class), Matchers.any(StringBuilder.class));
    Assert.assertTrue(controller.createJob(slurmFileName, "slurm",
        expectedCommand, WORKING_DIRECTORY, 1));
  }

  @Test
  public void testKillJob() throws Exception {
    // fail if job ids are not set
    List<String> jobIds = new ArrayList<>(Arrays.asList("0000"));
    String jobIdFile = "job.id";
    Mockito.doReturn(new ArrayList<>()).when(controller).readFromFile(Matchers.anyString());
    Assert.assertFalse(controller.killJob(jobIdFile));
    // fail if process creation fails
    Mockito.doReturn(false).when(controller).runProcess(Matchers.anyString(),
        Matchers.any(String[].class), Matchers.any(StringBuilder.class));
    Mockito.doReturn(jobIds).when(controller).readFromFile(jobIdFile);
    Assert.assertFalse(controller.killJob(jobIdFile));
    // happy path
    Mockito.doReturn(true).when(controller).runProcess(Matchers.anyString(),
        Matchers.any(String[].class), Matchers.any(StringBuilder.class));
    Mockito.doReturn(jobIds).when(controller).readFromFile(jobIdFile);
    Assert.assertTrue(controller.killJob(jobIdFile));
  }
}
