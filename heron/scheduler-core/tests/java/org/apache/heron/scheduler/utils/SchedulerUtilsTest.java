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

package org.apache.heron.scheduler.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.PackingTestUtils;
import org.apache.heron.spi.utils.ShellUtils;

import static org.mockito.Mockito.eq;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({FileUtils.class, ShellUtils.class, SchedulerUtils.class})
public class SchedulerUtilsTest {
  private static final String WORKING_DIR = "home";
  private static final String CORE_RELEASE_URI = "mock://uri:121/only#swan";
  private static final String CORE_RELEASE_DEST = "core";
  private static final String TOPOLOGY_URI = "mock://uri:121/ruo#xi";
  private static final String TOPOLOGY_DEST = "topology";
  private static final String TOPOLOGY_NAME = "topology_name";
  private static final boolean IS_DELETE_PACKAGE = true;
  private static final boolean IS_VERBOSE = true;

  @Test
  public void testConstructSchedulerResponse() throws Exception {
    Common.Status okStatus = Common.Status.newBuilder().
        setStatus(Common.StatusCode.OK)
        .build();
    Assert.assertEquals(okStatus, SchedulerUtils.constructSchedulerResponse(true).getStatus());

    Common.Status notOKStatus = Common.Status.newBuilder()
        .setStatus(Common.StatusCode.NOTOK)
        .build();
    Assert.assertEquals(notOKStatus, SchedulerUtils.constructSchedulerResponse(false).getStatus());
  }

  /**
   * Test curlAndExtractPackage()
   */
  @Test
  public void testCurlAndExtractPackage() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    PowerMockito.mockStatic(FileUtils.class);

    // Failed to curl the package
    PowerMockito.
        when(ShellUtils.curlPackage(TOPOLOGY_URI, TOPOLOGY_DEST, IS_VERBOSE, false)).
        thenReturn(false);
    Assert.assertFalse(SchedulerUtils.curlAndExtractPackage(
            WORKING_DIR, TOPOLOGY_URI, TOPOLOGY_DEST, IS_DELETE_PACKAGE, IS_VERBOSE));

    // Ok to curl package
    PowerMockito.
        when(ShellUtils.curlPackage(TOPOLOGY_URI, TOPOLOGY_DEST, IS_VERBOSE, false)).
        thenReturn(true);
    // Failed to extract package
    PowerMockito.
        when(ShellUtils.extractPackage(TOPOLOGY_DEST, WORKING_DIR, IS_VERBOSE, false)).
        thenReturn(false);
    Assert.assertFalse(SchedulerUtils.curlAndExtractPackage(
            WORKING_DIR, TOPOLOGY_URI, TOPOLOGY_DEST, IS_DELETE_PACKAGE, IS_VERBOSE));

    // Ok to curl and extract the package (inheritIO is off)
    PowerMockito.
        when(ShellUtils.curlPackage(TOPOLOGY_URI, TOPOLOGY_DEST, IS_VERBOSE, false)).
        thenReturn(true);
    PowerMockito.
        when(ShellUtils.extractPackage(TOPOLOGY_DEST, WORKING_DIR, IS_VERBOSE, false)).
        thenReturn(true);

    // Not required to delete the package
    boolean isToDeletePackage = false;
    Assert.assertTrue(SchedulerUtils.curlAndExtractPackage(
        WORKING_DIR, TOPOLOGY_URI, TOPOLOGY_DEST, isToDeletePackage, IS_VERBOSE));
    // deleteFile should not be invoked
    PowerMockito.verifyStatic(Mockito.never());
    FileUtils.deleteFile(Mockito.anyString());

    // the whole process should success even if failed to delete the package
    PowerMockito.when(FileUtils.deleteFile(Mockito.anyString())).thenReturn(false);

    Assert.assertTrue(SchedulerUtils.curlAndExtractPackage(
        WORKING_DIR, TOPOLOGY_URI, TOPOLOGY_DEST, true, IS_VERBOSE));
    // deleteFile should be invoked once
    PowerMockito.verifyStatic(Mockito.times(1));
    FileUtils.deleteFile(Mockito.anyString());
  }

  /**
   * Test method createOrCleanDirectory()
   */
  @Test
  public void testSetupWorkingDirectory() throws Exception {
    boolean isVerbose = true;

    PowerMockito.mockStatic(FileUtils.class);
    // work directory not exist
    PowerMockito.when(FileUtils.isDirectoryExists(Mockito.anyString())).thenReturn(false);

    // Failed to create dir
    PowerMockito.when(FileUtils.createDirectory(Mockito.anyString())).thenReturn(false);
    Assert.assertFalse(SchedulerUtils.createOrCleanDirectory(WORKING_DIR));
    // OK to create dir
    PowerMockito.when(FileUtils.createDirectory(Mockito.anyString())).thenReturn(true);

    // Fail to cleanup
    PowerMockito.when(FileUtils.cleanDir(Mockito.anyString())).thenReturn(false);
    Assert.assertFalse(SchedulerUtils.createOrCleanDirectory(WORKING_DIR));

    // Ok to cleanup
    PowerMockito.when(FileUtils.cleanDir(Mockito.anyString())).thenReturn(true);
    Assert.assertTrue(SchedulerUtils.createOrCleanDirectory(WORKING_DIR));
  }

  @Test
  public void testSchedulerCommandArgs() throws Exception {
    List<Integer> freePorts = new ArrayList<>();

    freePorts.add(1);
    String[] expectedArgs =
        {"--cluster", null, "--role", null,
            "--environment", null, "--topology_name", null,
            "--topology_bin", null, "--http_port", "1"};
    Assert.assertArrayEquals(expectedArgs, SchedulerUtils.schedulerCommandArgs(
        Mockito.mock(Config.class), Mockito.mock(Config.class), freePorts));
  }

  @Test
  public void persistUpdatedPackingPlanWillUpdatesStateManager() {
    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    Mockito.when(adaptor
        .updatePackingPlan(Mockito.any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME)))
        .thenReturn(true);

    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    containers.add(PackingTestUtils.testContainerPlan(1, 0, 1, 2));
    PackingPlan packing = new PackingPlan("id", containers);
    SchedulerUtils.persistUpdatedPackingPlan(TOPOLOGY_NAME, packing, adaptor);
    Mockito.verify(adaptor)
        .updatePackingPlan(Mockito.any(PackingPlans.PackingPlan.class), eq(TOPOLOGY_NAME));
  }
}
