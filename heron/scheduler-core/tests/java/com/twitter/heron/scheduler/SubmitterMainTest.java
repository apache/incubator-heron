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

import org.junit.Assert;
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
    Config config = Mockito.mock(Config.class);

    SchedulerStateManagerAdaptor adaptor = Mockito.mock(SchedulerStateManagerAdaptor.class);
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();
    SubmitterMain submitterMain = new SubmitterMain(config, topology);

    // Topology is running
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(true);
    Assert.assertFalse(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));

    // Topology is not running
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(null);
    Assert.assertTrue(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));
    Mockito.when(adaptor.isTopologyRunning(Mockito.eq(TOPOLOGY_NAME))).thenReturn(false);
    Assert.assertTrue(submitterMain.validateSubmit(adaptor, TOPOLOGY_NAME));
  }

  /**
   * Unit test submitTopology method
   * @throws Exception
   */
  @Test
  public void testSubmitTopology() throws Exception {
    // Mock objects to be verified
    IStateManager statemgr = Mockito.mock(IStateManager.class);
    ILauncher launcher = Mockito.mock(ILauncher.class);
    IPacking packing = Mockito.mock(IPacking.class);
    IUploader uploader = Mockito.mock(IUploader.class);

    // Mock ReflectionUtils stuff
    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(statemgr).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
    PowerMockito.doReturn(launcher).
        when(ReflectionUtils.class, "newInstance", LAUNCHER_CLASS);
    PowerMockito.doReturn(packing).
        when(ReflectionUtils.class, "newInstance", PACKING_CLASS);
    PowerMockito.doReturn(uploader).
        when(ReflectionUtils.class, "newInstance", UPLOADER_CLASS);

    Config config = Mockito.mock(Config.class);
    Mockito.
        when(config.getStringValue(ConfigKeys.get(STATE_MANAGER_CLASS))).
        thenReturn(STATE_MANAGER_CLASS);
    Mockito.
        when(config.getStringValue(ConfigKeys.get(LAUNCHER_CLASS))).
        thenReturn(LAUNCHER_CLASS);
    Mockito.
        when(config.getStringValue(ConfigKeys.get(PACKING_CLASS))).
        thenReturn(PACKING_CLASS);
    Mockito.
        when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).
        thenReturn(UPLOADER_CLASS);

    // Instances to test
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();
    SubmitterMain submitterMain = Mockito.spy(new SubmitterMain(config, topology));

    // Failed to instantiate
    final String CLASS_NOT_EXIST = "class_not_exist";
    Mockito.
        when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).
        thenReturn(CLASS_NOT_EXIST);
    Assert.assertFalse(submitterMain.submitTopology());
    Mockito.verify(uploader, Mockito.never()).close();
    Mockito.verify(packing, Mockito.never()).close();
    Mockito.verify(launcher, Mockito.never()).close();
    Mockito.verify(statemgr, Mockito.never()).close();

    // OK to instantiate all resources
    Mockito.
        when(config.getStringValue(ConfigKeys.get(UPLOADER_CLASS))).
        thenReturn(UPLOADER_CLASS);

    // Failed to validate the submission
    Mockito.doReturn(false).when(submitterMain).
        validateSubmit(Mockito.any(SchedulerStateManagerAdaptor.class), Mockito.anyString());
    Assert.assertFalse(submitterMain.submitTopology());
    // Resources should be closed even the submission failed
    Mockito.verify(uploader, Mockito.atLeastOnce()).close();
    Mockito.verify(packing, Mockito.atLeastOnce()).close();
    Mockito.verify(launcher, Mockito.atLeastOnce()).close();
    Mockito.verify(statemgr, Mockito.atLeastOnce()).close();

    // validated the submission
    Mockito.doReturn(true).when(submitterMain).
        validateSubmit(Mockito.any(SchedulerStateManagerAdaptor.class), Mockito.anyString());

    // Failed to upload package, return null
    Mockito.doReturn(null).when(submitterMain).
        uploadPackage(Mockito.eq(uploader));
    Assert.assertFalse(submitterMain.submitTopology());
    // Should not invoke undo
    Mockito.verify(uploader, Mockito.never()).undo();

    // OK to upload package
    final URI packageURI = new URI("mock://uri:924/x#ke");
    Mockito.doReturn(packageURI).when(submitterMain).
        uploadPackage(Mockito.eq(uploader));

    // Failed to callLauncherRunner
    Mockito.doReturn(false).when(submitterMain).
        callLauncherRunner(Mockito.any(Config.class));
    Assert.assertFalse(submitterMain.submitTopology());
    // Should invoke undo
    Mockito.verify(uploader).undo();

    // Happy path
    Mockito.doReturn(true).when(submitterMain).
        callLauncherRunner(Mockito.any(Config.class));
    Assert.assertTrue(submitterMain.submitTopology());
  }
}
