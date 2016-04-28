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

import org.junit.After;
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
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;

import org.junit.Assert;


/**
 * LocalFileSystemStateManager Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class LocalFileSystemStateManagerTest {
  private static final String topologyName = "topologyName";
  private static final String rootAddr = "/";
  private Config config;

  @Before
  public void before() throws Exception {
    config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(), rootAddr)
        .put(LocalFileSystemKeys.initializeFileTree(), false)
        .build();
  }

  @After
  public void after() throws Exception {
  }

  public Config getConfig() {
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
        setTopologyName(topologyName).
        build();
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(location.toByteArray()).
        when(FileUtils.class, "readFromFile", Matchers.anyString());

    Scheduler.SchedulerLocation locationFetched =
        manager.getSchedulerLocation(null, topologyName).get();

    Assert.assertEquals(location, locationFetched);
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
        when(FileUtils.class, "writeToFile", Matchers.anyString(), Matchers.any(byte[].class));

    Assert.assertTrue(manager.setExecutionState(defaultState, "").get());

    PowerMockito.verifyStatic();
    FileUtils.writeToFile(Matchers.eq(String.format("%s/%s/%s",
            rootAddr, "executionstate", defaultState.getTopologyName())),
        Matchers.eq(defaultState.toByteArray()));
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
        when(FileUtils.class, "writeToFile", Matchers.anyString(), Matchers.any(byte[].class));

    Assert.assertTrue(manager.setTopology(topology, topologyName).get());

    PowerMockito.verifyStatic();
    FileUtils.writeToFile(
        Matchers.eq(String.format("%s/%s/%s",
            rootAddr, "topologies", topologyName)),
        Matchers.eq(topology.toByteArray()));
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

    Assert.assertTrue(manager.deleteExecutionState(topologyName).get());

    PowerMockito.verifyStatic();
    FileUtils.deleteFile(Matchers.eq(String.format("%s/%s/%s",
        rootAddr, "executionstate", topologyName)));
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

    Assert.assertTrue(manager.deleteTopology(topologyName).get());

    FileUtils.deleteFile(Matchers.eq(String.format("%s/%s/%s",
        rootAddr, "topologies", topologyName)));
  }
}
