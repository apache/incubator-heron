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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.utils.ReflectionUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
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

  @Test
  public void testValidateSubmit() throws Exception {
    Config config = mock(Config.class);

    SchedulerStateManagerAdaptor adaptor = mock(SchedulerStateManagerAdaptor.class);
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();
    SubmitterMain submitterMain = new SubmitterMain(config, topology);

    // Topology is running
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(true);
    assertFalse(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));

    // Topology is not running
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(null);
    assertTrue(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));
    when(adaptor.isTopologyRunning(eq(TOPOLOGY_NAME))).thenReturn(false);
    assertTrue(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));
  }

  /**
   * Unit test submitTopology method
   * @throws Exception
   */
  @Test
  public void testSubmitTopology() throws Exception {
    // Mock objects to be verified
    IStateManager statemgr = mock(IStateManager.class);
    ILauncher launcher = mock(ILauncher.class);
    IPacking packing = mock(IPacking.class);
    IUploader uploader = mock(IUploader.class);

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

    Config config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get(STATE_MANAGER_CLASS)))
        .thenReturn(STATE_MANAGER_CLASS);
    when(config.getStringValue(ConfigKeys.get(LAUNCHER_CLASS)))
        .thenReturn(LAUNCHER_CLASS);
    when(config.getStringValue(ConfigKeys.get(PACKING_CLASS)))
        .thenReturn(PACKING_CLASS);
    when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS)))
        .thenReturn(UPLOADER_CLASS);

    // Instances to test
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();
    SubmitterMain submitterMain = spy(new SubmitterMain(config, topology));

    // Failed to instantiate
    final String CLASS_NOT_EXIST = "class_not_exist";
    when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).thenReturn(CLASS_NOT_EXIST);
    assertFalse(submitterMain.submitTopology());
    verify(uploader, never()).close();
    verify(launcher, never()).close();
    verify(statemgr, never()).close();

    // OK to instantiate all resources
    when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).thenReturn(UPLOADER_CLASS);

    // Failed to validate the submission
    doReturn(false).when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());
    assertFalse(submitterMain.submitTopology());
    // Resources should be closed even the submission failed
    verify(uploader, atLeastOnce()).close();
    verify(launcher, atLeastOnce()).close();
    verify(statemgr, atLeastOnce()).close();

    // validated the submission
    doReturn(true).when(submitterMain)
        .validateSubmit(any(SchedulerStateManagerAdaptor.class), anyString());

    // Failed to upload package, return null
    doReturn(null).when(submitterMain).uploadPackage(eq(uploader));
    assertFalse(submitterMain.submitTopology());
    // Should not invoke undo
    verify(uploader, never()).undo();

    // OK to upload package
    final URI packageURI = new URI("mock://uri:924/x#ke");
    doReturn(packageURI).when(submitterMain).uploadPackage(eq(uploader));

    // Failed to callLauncherRunner
    doReturn(false).when(submitterMain).callLauncherRunner(Mockito.any(Config.class));
    assertFalse(submitterMain.submitTopology());
    // Should invoke undo
    verify(uploader).undo();

    // Happy path
    doReturn(true).when(submitterMain).callLauncherRunner(Mockito.any(Config.class));
    assertTrue(submitterMain.submitTopology());
  }
}
