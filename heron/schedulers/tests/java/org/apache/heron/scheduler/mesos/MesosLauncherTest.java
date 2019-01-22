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

package org.apache.heron.scheduler.mesos;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;


public class MesosLauncherTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testGetSchedulerCommand() throws Exception {
    final String JAVA_HOME = "java_home";
    final String NATIVE_LIBRARY_PATH = "native_library_path";
    final URI mockURI = TypeUtils.getURI("file:///mock/uri");

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getStringValue(MesosContext.SCHEDULER_WORKING_DIRECTORY))
        .thenReturn("working-dir");
    Mockito.when(config.getStringValue(MesosContext.HERON_MESOS_NATIVE_LIBRARY_PATH))
        .thenReturn(NATIVE_LIBRARY_PATH);
    Mockito.when(config.getStringValue(Key.JAVA_HOME))
        .thenReturn(JAVA_HOME);


    Config runtime = Mockito.mock(Config.class);
    Mockito.when(runtime.get(Key.TOPOLOGY_PACKAGE_URI)).thenReturn(mockURI);

    MesosLauncher launcher = Mockito.spy(MesosLauncher.class);
    String[] mockCommand = new String[]{"mock", "scheduler", "command"};
    Mockito.doReturn(mockCommand).when(launcher)
        .schedulerCommandArgs(Mockito.anyList());
    launcher.initialize(config, runtime);

    String[] command = launcher.getSchedulerCommand();

    // Assert the content of command
    Assert.assertEquals(String.format("%s/bin/java", JAVA_HOME), command[0]);
    Assert.assertEquals(
        String.format("-Djava.library.path=%s", NATIVE_LIBRARY_PATH), command[1]);

    Assert.assertEquals("-cp", command[2]);
    Assert.assertEquals(
        String.format("-Pheron.package.topology.uri=%s", mockURI.toString()), command[5]);
    Assert.assertEquals(mockCommand[0], command[6]);
    Assert.assertEquals(mockCommand[1], command[7]);
    Assert.assertEquals(mockCommand[2], command[8]);
  }

  @Test
  public void testLaunch() throws Exception {
    MesosLauncher launcher = Mockito.spy(MesosLauncher.class);
    PackingPlan packingPlan = Mockito.mock(PackingPlan.class);
    Config config = Mockito.mock(Config.class);
    Mockito.doReturn("working-dir").
        when(config).getStringValue(MesosContext.SCHEDULER_WORKING_DIRECTORY);
    Config runtime = Mockito.mock(Config.class);
    launcher.initialize(config, runtime);

    // Failed to setup working dir
    Mockito.doReturn(false).when(launcher).setupWorkingDirectory();
    Assert.assertFalse(launcher.launch(packingPlan));

    Mockito.doReturn(true).when(launcher).setupWorkingDirectory();
    String[] mockCommand = new String[]{"mock", "scheduler", "command"};
    Mockito.doReturn(mockCommand).when(launcher).getSchedulerCommand();

    // Failed to spwan the process
    Mockito.doReturn(null).when(launcher).startScheduler(mockCommand);
    Assert.assertFalse(launcher.launch(packingPlan));

    // Happy path
    Process p = Mockito.mock(Process.class);
    Mockito.doReturn(p).when(launcher).startScheduler(mockCommand);
    Assert.assertTrue(launcher.launch(packingPlan));
  }
}
