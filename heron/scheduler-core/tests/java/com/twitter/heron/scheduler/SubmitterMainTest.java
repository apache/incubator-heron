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

package com.twitter.heron.scheduler;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.dryrun.SubmitDryRunResponse;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.uploader.UploaderException;
import com.twitter.heron.spi.utils.ReflectionUtils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ReflectionUtils.class)
public class SubmitterMainTest {
  private static final String TOPOLOGY_NAME = "topologyName";

  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private static final String LAUNCHER_CLASS = "LAUNCHER_CLASS";
  private static final String PACKING_CLASS = "PACKING_CLASS";
  private static final String UPLOADER_CLASS = "UPLOADER_CLASS";

  private IStateManager statemgr;
  private ILauncher launcher;
  private IPacking packing;
  private IUploader uploader;

  private Config config;

  private TopologyAPI.Topology topology;

  @Before
  public void setUp() throws Exception {
    // Mock objects to be verified
    statemgr = mock(IStateManager.class);
    launcher = mock(ILauncher.class);
    packing = mock(IPacking.class);
    uploader = mock(IUploader.class);

    // Mock ReflectionUtils stuff
    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(statemgr)
        .when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
    PowerMockito.doReturn(launcher)
        .when(ReflectionUtils.class, "newInstance", LAUNCHER_CLASS);
    PowerMockito.doReturn(packing)
        .when(ReflectionUtils.class, "newInstance", PACKING_CLASS);
    PowerMockito.doReturn(uploader)
        .when(ReflectionUtils.class, "newInstance", UPLOADER_CLASS);

    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get(STATE_MANAGER_CLASS)))
        .thenReturn(STATE_MANAGER_CLASS);
    when(config.getStringValue(ConfigKeys.get(LAUNCHER_CLASS)))
        .thenReturn(LAUNCHER_CLASS);
    when(config.getStringValue(ConfigKeys.get(PACKING_CLASS)))
        .thenReturn(PACKING_CLASS);
    when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS)))
        .thenReturn(UPLOADER_CLASS);

    topology = TopologyAPI.Topology.getDefaultInstance();
  }

  @Test
  public void testValidateSubmit() throws Exception {
    SubmitterMain submitterMain = new SubmitterMain(config, topology);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    // Topology is not running
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(null);
    submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME);
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(false);
    submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME);
  }

  @Test(expected = TopologySubmissionException.class)
  public void testValidateSubmitAlreadyRunning() throws Exception {
    SubmitterMain submitterMain = new SubmitterMain(config, topology);
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    // Topology is running
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(true);
    submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME);
  }

  @Test(expected = TopologySubmissionException.class)
  public void testSubmitTopologyAlreadyRunning() throws Exception {
    // Topology is running
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    doThrow(new TopologySubmissionException("")).when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    try {
      submitterMain.submitTopology();
    } finally {
      verify(uploader, atLeastOnce()).close();
      verify(launcher, atLeastOnce()).close();
      verify(statemgr, atLeastOnce()).close();
    }
  }

  @Test(expected = UploaderException.class)
  public void testSubmitTopologyClassNotExist() throws Exception {
    final String CLASS_NOT_EXIST = "class_not_exist";
    when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).thenReturn(CLASS_NOT_EXIST);
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    doNothing().when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    try {
      submitterMain.submitTopology();
    } finally {
      verify(uploader, never()).close();
      verify(launcher, never()).close();
      verify(statemgr, never()).close();
      when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).thenReturn(UPLOADER_CLASS);
    }
  }

  @Test(expected = UploaderException.class)
  public void testSubmitTopologyUploaderException() throws Exception {
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    doNothing().when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    doThrow(new UploaderException("")).when(submitterMain).uploadPackage(eq(uploader));
    try {
      submitterMain.submitTopology();
    } finally {
      verify(uploader, never()).undo();
      verify(uploader).close();
      verify(launcher).close();
      verify(statemgr).close();
    }
  }

  @Test(expected = PackingException.class)
  public void testSubmitTopologyLauncherException() throws Exception {
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    doNothing().when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    final URI packageURI = new URI("mock://uri:924/x#ke");
    doReturn(packageURI).when(submitterMain).uploadPackage(eq(uploader));
    doThrow(new PackingException("")).when(submitterMain)
        .callLauncherRunner(Mockito.any(Config.class));
    submitterMain.submitTopology();
  }

  @Test(expected = SubmitDryRunResponse.class)
  public void testSubmitTopologyDryRun() throws Exception {
    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    PowerMockito.whenNew(SchedulerStateManagerAdaptor.class).withAnyArguments().
        thenReturn(adaptor);
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    when(config.getBooleanValue(ConfigKeys.get("DRY_RUN"), false)).thenReturn(true);
    try {
      submitterMain.submitTopology();
    } finally {
      /* under dry-run mode, the program should not
         1. upload topology package
         2. validate that topology is not running
       */
      verify(uploader, never()).uploadPackage();
      verify(adaptor, never()).isTopologyRunning(anyString());
    }
  }

  @Test
  public void testSubmitTopologySuccessful() throws Exception {
    when(config.getBooleanValue(ConfigKeys.get("DRY_RUN"))).thenReturn(false);
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));
    doNothing().when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    doNothing().when(submitterMain).callLauncherRunner(Mockito.any(Config.class));
    submitterMain.submitTopology();
  }
}
