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

package com.twitter.heron.statemgr.localfs;

import com.google.common.util.concurrent.ListenableFuture;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.common.Keys;

/**
 * LocalFileSystemStateManager Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class LocalFileSystemStateManagerTest {

  private static final String TOPOLOGY_NAME = "topologyName";
  private static final String ROOT_ADDR = "/";
  private SpiCommonConfig config;

  @Before
  public void before() throws Exception {
    config = SpiCommonConfig.newBuilder()
        .put(Keys.stateManagerRootPath(), ROOT_ADDR)
        .put(LocalFileSystemKeys.initializeFileTree(), false)
        .build();
  }

  @After
  public void after() throws Exception {
  }

  public SpiCommonConfig getConfig() {
    return config;
  }

  @Test
  public void testInitialize() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).
        when(FileUtils.class, "createDirectory", Matchers.anyString());

    Assert.assertTrue(manager.initTree());

    PowerMockito.verifyStatic(Mockito.atLeastOnce());
    FileUtils.createDirectory(Matchers.anyString());
  }

  /**
   * Method: getSchedulerLocation(String topologyName, WatchCallback watcher)
   */
  @Test
  public void testGetSchedulerLocation() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    Scheduler.SchedulerLocation location = Scheduler.SchedulerLocation.newBuilder().
        setHttpEndpoint("host:1").
        setTopologyName(TOPOLOGY_NAME).
        build();
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(location.toByteArray()).
        when(FileUtils.class, "readFromFile", Matchers.anyString());

    Scheduler.SchedulerLocation locationFetched =
        manager.getSchedulerLocation(null, TOPOLOGY_NAME).get();

    Assert.assertEquals(location, locationFetched);
  }

  /**
   * Method: setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName)
   */
  @Test
  public void testSetSchedulerLocation() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    Mockito.doReturn(Mockito.mock(ListenableFuture.class)).when(manager).
        setData(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyBoolean());

    manager.setSchedulerLocation(Scheduler.SchedulerLocation.getDefaultInstance(), "");
    Mockito.verify(manager).
        setData(Mockito.anyString(), Mockito.any(byte[].class), Mockito.eq(true));
  }

  /**
   * Method: setExecutionState(ExecutionEnvironment.ExecutionState executionState)
   */
  @Test
  public void testSetExecutionState() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).
        when(FileUtils.class, "createDirectory", Matchers.anyString());

    Assert.assertTrue(manager.initTree());

    ExecutionEnvironment.ExecutionState defaultState =
        ExecutionEnvironment.ExecutionState.getDefaultInstance();

    PowerMockito.doReturn(true).
        when(FileUtils.class, "writeToFile", Mockito.anyString(), Mockito.any(byte[].class),
            Mockito.anyBoolean());

    Assert.assertTrue(manager.setExecutionState(defaultState, "").get());

    PowerMockito.verifyStatic();
    FileUtils.writeToFile(Matchers.eq(String.format("%s/%s/%s",
            ROOT_ADDR, "executionstate", defaultState.getTopologyName())),
        Matchers.eq(defaultState.toByteArray()), Mockito.eq(false));
  }

  /**
   * Method: setTopology(TopologyAPI.Topology topology)
   */
  @Test
  public void testSetTopology() throws Exception {
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).
        when(FileUtils.class, "createDirectory", Matchers.anyString());

    Assert.assertTrue(manager.initTree());

    PowerMockito.doReturn(true).
        when(FileUtils.class, "writeToFile", Mockito.anyString(), Mockito.any(byte[].class),
            Mockito.anyBoolean());

    Assert.assertTrue(manager.setTopology(topology, TOPOLOGY_NAME).get());

    PowerMockito.verifyStatic();
    FileUtils.writeToFile(
        Matchers.eq(String.format("%s/%s/%s",
            ROOT_ADDR, "topologies", TOPOLOGY_NAME)),
        Matchers.eq(topology.toByteArray()), Mockito.eq(false));
  }

  /**
   * Method: deleteExecutionState()
   */
  @Test
  public void testDeleteExecutionState() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).
        when(FileUtils.class, "createDirectory", Matchers.anyString());

    Assert.assertTrue(manager.initTree());

    PowerMockito.doReturn(true).
        when(FileUtils.class, "deleteFile", Matchers.anyString());

    Assert.assertTrue(manager.deleteExecutionState(TOPOLOGY_NAME).get());

    PowerMockito.verifyStatic();
    FileUtils.deleteFile(Matchers.eq(String.format("%s/%s/%s",
        ROOT_ADDR, "executionstate", TOPOLOGY_NAME)));
  }

  /**
   * Method: deleteTopology()
   */
  @Test
  public void testDeleteTopology() throws Exception {
    LocalFileSystemStateManager manager =
        Mockito.spy(new LocalFileSystemStateManager());
    manager.initialize(getConfig());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).
        when(FileUtils.class, "createDirectory", Matchers.anyString());

    Assert.assertTrue(manager.initTree());

    PowerMockito.doReturn(true).
        when(FileUtils.class, "deleteFile", Matchers.anyString());

    Assert.assertTrue(manager.deleteTopology(TOPOLOGY_NAME).get());

    FileUtils.deleteFile(Matchers.eq(String.format("%s/%s/%s",
        ROOT_ADDR, "topologies", TOPOLOGY_NAME)));
  }
}
