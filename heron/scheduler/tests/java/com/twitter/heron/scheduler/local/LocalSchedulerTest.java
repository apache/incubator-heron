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

package com.twitter.heron.scheduler.local;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;

/**
 * LocalScheduler Tester.
 */
public class LocalSchedulerTest {

  private static final int shards = 10;
  private static final String stateMgrClass = "com.twitter.heron.statemgr.NullStateManager";

  DefaultConfigLoader createRequiredConfig() throws Exception {
    DefaultConfigLoader schedulerConfig = DefaultConfigLoader.class.newInstance();
    schedulerConfig.addDefaultProperties();
    schedulerConfig.properties.setProperty(LocalConfig.WORKING_DIRECTORY,
        LocalConfig.WORKING_DIRECTORY);
    schedulerConfig.properties.put(LocalConfig.NUM_SHARDS, "" + shards);
    schedulerConfig.properties.setProperty(Constants.STATE_MANAGER_CLASS, stateMgrClass);
    return schedulerConfig;
  }

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: initialize(IConfig schedulerConfig)
   */
  @Test
  public void testInitialize() throws Exception {
    DefaultConfigLoader config = createRequiredConfig();

    LocalScheduler scheduler = Mockito.spy(LocalScheduler.class.newInstance());

    Mockito.doNothing().when(scheduler).startExecutor(Matchers.anyInt());

    LaunchContext context =
        new LaunchContext(config, TopologyAPI.Topology.getDefaultInstance());

    scheduler.initialize(context);

    InOrder inOrder = Mockito.inOrder(scheduler);
    for (int i = 0; i < shards; i++) {
      inOrder.verify(scheduler).startExecutor(i);
    }
    Mockito.verify(scheduler, Mockito.times(shards)).startExecutor(Matchers.anyInt());

  }
}
