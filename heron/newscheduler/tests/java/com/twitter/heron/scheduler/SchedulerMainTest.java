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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.service.server.SchedulerServer;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.IPackingAlgorithm;
import com.twitter.heron.spi.packing.NullPackingAlgorithm;
import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.NullLauncher;
import com.twitter.heron.spi.scheduler.NullScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.uploader.NullUploader;
import com.twitter.heron.spi.util.Factory;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SchedulerMain.class, Factory.class})
public class SchedulerMainTest {

    private IConfigLoader createConfig() {
        IConfigLoader config = mock(DefaultConfigLoader.class);
        when(config.getUploaderClass()).thenReturn(NullUploader.class.getName());
        when(config.getLauncherClass()).thenReturn(NullLauncher.class.getName());
        when(config.getSchedulerClass()).thenReturn(NullScheduler.class.getName());
        when(config.getPackingAlgorithmClass()).thenReturn(NullPackingAlgorithm.class.getName());
        when(config.load(anyString(), anyString())).thenReturn(true);
        return config;
    }

    @Test
    public void testSchedulerMainWorkflow() throws Exception {
        SchedulerServer server = mock(SchedulerServer.class);
        IConfigLoader config = createConfig();
        String configLoader = config.getClass().getName();
        String schedulerClass = config.getSchedulerClass();
        String packingAlgorithmClass = config.getPackingAlgorithmClass();
        PowerMockito.spy(SchedulerMain.class);
        PowerMockito.spy(Factory.class);
        PowerMockito.doReturn(config).when(Factory.class, "makeConfigLoader", eq(configLoader));
        PowerMockito.doReturn(mock(IStateManager.class)).when(Factory.class, "makeStateManager", anyString());
        IScheduler scheduler = spy(new NullScheduler());
        PowerMockito.doReturn(scheduler).when(Factory.class, "makeScheduler", eq(schedulerClass));
        IPackingAlgorithm packingAlgorithm = spy(new NullPackingAlgorithm());
        PowerMockito.doReturn(packingAlgorithm).when(Factory.class, "makePackingAlgorithm", eq(packingAlgorithmClass));

        PowerMockito.doReturn(server).when(SchedulerMain.class, "runServer", any(IScheduler.class), any(LaunchContext.class), anyInt());
        PowerMockito.doNothing().when(SchedulerMain.class,
                "setSchedulerLocation",
                any(LaunchContext.class),
                any(SchedulerServer.class));
        SchedulerMain.runScheduler(config.getSchedulerClass(), configLoader, "", NetworkUtility.getFreePort(), "", TopologyAPI.Topology.getDefaultInstance());
        verify(scheduler, times(1)).initialize(any(LaunchContext.class));
        verify(scheduler, atLeastOnce()).schedule(any(PackingPlan.class));
    }
}

