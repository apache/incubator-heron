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

package org.apache.heron.statemgr.localfs;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * LocalFileSystemStateManager Tester.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(FileUtils.class)
public class LocalFileSystemStateManagerTest {

  private static final String TOPOLOGY_NAME = "topologyName";
  private static final IStateManager.LockName LOCK_NAME = IStateManager.LockName.UPDATE_TOPOLOGY;
  private static final String ROOT_ADDR = "/";
  private LocalFileSystemStateManager manager;

  @Before
  public void before() throws Exception {
    manager = initSpyManager(ROOT_ADDR, false);
  }

  private static LocalFileSystemStateManager initSpyManager(String rootPath, boolean initTree) {
    Config config = Config.newBuilder()
        .put(Key.STATEMGR_ROOT_PATH, rootPath)
        .put(LocalFileSystemKey.IS_INITIALIZE_FILE_TREE.value(), initTree)
        .build();
    LocalFileSystemStateManager manager = spy(new LocalFileSystemStateManager());
    manager.initialize(config);
    return manager;
  }

  @After
  public void after() throws Exception {
  }

  private void initMocks() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).when(FileUtils.class, "createDirectory", anyString());
    PowerMockito.doReturn(true).when(FileUtils.class, "isFileExists", anyString());

    assertTrue(manager.initTree());

    PowerMockito.doReturn(true)
        .when(FileUtils.class, "writeToFile", anyString(), any(byte[].class), anyBoolean());

    PowerMockito.doReturn(true).when(FileUtils.class, "deleteFile", anyString());
  }

  @Test
  public void testInitialize() throws Exception {
    initMocks();

    PowerMockito.verifyStatic(atLeastOnce());
    FileUtils.createDirectory(anyString());
  }

  /**
   * Method: getSchedulerLocation(String topologyName, WatchCallback watcher)
   */
  @Test
  public void testGetSchedulerLocation() throws Exception {
    Scheduler.SchedulerLocation location = Scheduler.SchedulerLocation.newBuilder()
        .setHttpEndpoint("host:1")
        .setTopologyName(TOPOLOGY_NAME)
        .build();
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).when(FileUtils.class, "isFileExists", anyString());
    PowerMockito.doReturn(location.toByteArray()).
        when(FileUtils.class, "readFromFile", anyString());

    Scheduler.SchedulerLocation locationFetched =
        manager.getSchedulerLocation(null, TOPOLOGY_NAME).get();

    assertEquals(location, locationFetched);
  }

  /**
   * Method: setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName)
   */
  @Test
  public void testSetSchedulerLocation() throws Exception {
    doReturn(mock(ListenableFuture.class)).when(manager)
        .setData(anyString(), any(byte[].class), anyBoolean());

    manager.setSchedulerLocation(Scheduler.SchedulerLocation.getDefaultInstance(), "");
    verify(manager).setData(anyString(), any(byte[].class), eq(true));
  }

  /**
   * Method: setExecutionState(ExecutionEnvironment.ExecutionState executionState)
   */
  @Test
  public void testSetExecutionState() throws Exception {
    initMocks();
    ExecutionEnvironment.ExecutionState defaultState =
        ExecutionEnvironment.ExecutionState.getDefaultInstance();

    assertTrue(manager.setExecutionState(defaultState, "").get());

    assertWriteToFile(
        String.format("%s/%s/%s", ROOT_ADDR, "executionstate", defaultState.getTopologyName()),
        defaultState.toByteArray(), false);
  }

  /**
   * Method: setTopology(TopologyAPI.Topology topology)
   */
  @Test
  public void testSetTopology() throws Exception {
    initMocks();
    TopologyAPI.Topology topology = TopologyAPI.Topology.getDefaultInstance();

    assertTrue(manager.setTopology(topology, TOPOLOGY_NAME).get());

    assertWriteToFile(
        String.format("%s/%s/%s", ROOT_ADDR, "topologies", TOPOLOGY_NAME),
        topology.toByteArray(), false);
  }

  @Test
  public void testSetPackingPlan() throws Exception {
    initMocks();
    PackingPlans.PackingPlan packingPlan = PackingPlans.PackingPlan.getDefaultInstance();

    assertTrue(manager.setPackingPlan(packingPlan, TOPOLOGY_NAME).get());

    assertWriteToFile(
        String.format("%s/%s/%s", ROOT_ADDR, "packingplans", TOPOLOGY_NAME),
        packingPlan.toByteArray(), true);
  }

  /**
   * Method: deleteExecutionState()
   */
  @Test
  public void testDeleteExecutionState() throws Exception {
    initMocks();

    assertTrue(manager.deleteExecutionState(TOPOLOGY_NAME).get());

    assertDeleteFile(String.format("%s/%s/%s", ROOT_ADDR, "executionstate", TOPOLOGY_NAME));
  }

  /**
   * Method: deleteTopology()
   */
  @Test
  public void testDeleteTopology() throws Exception {
    initMocks();

    assertTrue(manager.deleteTopology(TOPOLOGY_NAME).get());

    assertDeleteFile(String.format("%s/%s/%s", ROOT_ADDR, "topologies", TOPOLOGY_NAME));
  }

  @Test
  public void testDeletePackingPlan() throws Exception {
    initMocks();

    assertTrue(manager.deletePackingPlan(TOPOLOGY_NAME).get());

    assertDeleteFile(String.format("%s/%s/%s", ROOT_ADDR, "packingplans", TOPOLOGY_NAME));
  }

  @Test
  public void testGetLock() throws Exception {
    initMocks();
    String expectedLockPath = String.format("//locks/%s__%s", TOPOLOGY_NAME, LOCK_NAME.getName());
    byte[] expectedContents = Thread.currentThread().getName().getBytes(Charset.defaultCharset());

    Lock lock = manager.getLock(TOPOLOGY_NAME, LOCK_NAME);

    assertTrue(lock.tryLock(0, TimeUnit.MILLISECONDS));
    assertWriteToFile(expectedLockPath, expectedContents, false);

    lock.unlock();
    assertDeleteFile(expectedLockPath);
  }

  @Test
  public void testGetFilesystemLock() throws Exception {
    Path tempDir = Files.createTempDirectory("heron-testGetFilesystemLock");
    LocalFileSystemStateManager fsBackedManager = initSpyManager(tempDir.toString(), true);
    Lock lock = fsBackedManager.getLock(TOPOLOGY_NAME, LOCK_NAME);
    assertTrue("Failed to get lock", lock.tryLock(0, TimeUnit.MILLISECONDS));
    lock.unlock();
  }

  @Test
  public void testLockTaken() throws Exception {
    String expectedLockPath = String.format("//locks/%s__%s", TOPOLOGY_NAME, LOCK_NAME.getName());
    byte[] expectedContents = Thread.currentThread().getName().getBytes(Charset.defaultCharset());

    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(false)
        .when(FileUtils.class, "writeToFile", anyString(), any(byte[].class), anyBoolean());

    Lock lock = manager.getLock(TOPOLOGY_NAME, LOCK_NAME);

    assertFalse(lock.tryLock(0, TimeUnit.MILLISECONDS));
    assertWriteToFile(expectedLockPath, expectedContents, false);
  }

  private static void assertWriteToFile(String path, byte[] bytes, boolean overwrite) {
    PowerMockito.verifyStatic(times(1));
    FileUtils.writeToFile(eq(path), eq(bytes), eq(overwrite));
  }

  private static void assertDeleteFile(String path) {
    PowerMockito.verifyStatic(times(1));
    FileUtils.deleteFile(eq(path));
  }
}
