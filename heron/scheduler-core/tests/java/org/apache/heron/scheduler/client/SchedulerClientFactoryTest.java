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

package org.apache.heron.scheduler.client;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.scheduler.SchedulerException;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.ReflectionUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
public class SchedulerClientFactoryTest {
  private static final String TOPOLOGY_NAME = "shiwei_0924_jiayou";

  @Test(expected = SchedulerException.class)
  public void testGetServiceSchedulerClientFail() throws Exception {
    // Instantiate mock objects
    Config config = Mockito.mock(Config.class);
    Config runtime = Mockito.mock(Config.class);
    SchedulerStateManagerAdaptor statemgr = Mockito.mock(SchedulerStateManagerAdaptor.class);

    // Get a ServiceSchedulerClient
    Mockito.when(config.getBooleanValue(Key.SCHEDULER_IS_SERVICE)).thenReturn(true);

    // Mock the runtime object
    Mockito.when(runtime.get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR)).thenReturn(statemgr);
    Mockito.when(runtime.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);

    // Failed to getSchedulerLocation
    Mockito.when(statemgr.getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME))).thenReturn(null);
    try {
      new SchedulerClientFactory(config, runtime).getSchedulerClient();
    } finally {
      Mockito.verify(statemgr).getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME));
    }
  }

  @Test
  public void testGetServiceSchedulerClientOk() {
    // Instantiate mock objects
    Config config = Mockito.mock(Config.class);
    Config runtime = Mockito.mock(Config.class);
    SchedulerStateManagerAdaptor statemgr = Mockito.mock(SchedulerStateManagerAdaptor.class);

    // Get a ServiceSchedulerClient
    Mockito.when(config.getBooleanValue(Key.SCHEDULER_IS_SERVICE)).thenReturn(true);

    // Mock the runtime object
    Mockito.when(runtime.get(Key.SCHEDULER_STATE_MANAGER_ADAPTOR)).thenReturn(statemgr);
    Mockito.when(runtime.getStringValue(Key.TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);

    // Get a schedulerLocation successfully
    Mockito.when(statemgr.getSchedulerLocation(Mockito.eq(TOPOLOGY_NAME))).
        thenReturn(Scheduler.SchedulerLocation.getDefaultInstance());
    Assert.assertNotNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
  }

  @Test
  @PrepareForTest(ReflectionUtils.class)
  public void testGetLibrarySchedulerClient() throws Exception {
    // Instantiate mock objects
    Config config = Mockito.mock(Config.class);
    Config runtime = Mockito.mock(Config.class);

    // Return a MockScheduler
    Mockito.when(config.getStringValue(Key.SCHEDULER_CLASS))
        .thenReturn(IScheduler.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IScheduler.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IScheduler.class.getName()));

    // Get a LibrarySchedulerClient
    Mockito.when(config.getBooleanValue(Key.SCHEDULER_IS_SERVICE)).thenReturn(false);
    Assert.assertNotNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
  }

  @Test(expected = SchedulerException.class)
  @PrepareForTest(ReflectionUtils.class)
  public void testGetLibrarySchedulerClientNotExist() throws Exception {
    // Instantiate mock objects
    Config config = Mockito.mock(Config.class);
    Config runtime = Mockito.mock(Config.class);

    // Return a MockScheduler
    Mockito.when(config.getStringValue(Key.SCHEDULER_CLASS))
        .thenReturn(IScheduler.class.getName());
    PowerMockito.mockStatic(ReflectionUtils.class);
    PowerMockito.doReturn(Mockito.mock(IScheduler.class))
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(IScheduler.class.getName()));
    // Return some scheduler class not exist
    final String SCHEDULER_CLASS_NOT_EXIST = "class_not_exist";
    Mockito.when(config.getStringValue(Key.SCHEDULER_CLASS)).
        thenReturn(SCHEDULER_CLASS_NOT_EXIST);
    PowerMockito.doThrow(new ClassNotFoundException())
        .when(ReflectionUtils.class, "newInstance", Mockito.eq(SCHEDULER_CLASS_NOT_EXIST));
    Assert.assertNull(new SchedulerClientFactory(config, runtime).getSchedulerClient());
  }
}
