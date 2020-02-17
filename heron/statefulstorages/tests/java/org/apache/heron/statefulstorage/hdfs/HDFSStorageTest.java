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

package org.apache.heron.statefulstorage.hdfs;


import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.CheckpointInfo;
import org.apache.heron.statefulstorage.StatefulStorageTestContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({FileSystem.class, CheckpointManager.InstanceStateCheckpoint.class})
public class HDFSStorageTest {

  private PhysicalPlans.Instance instance;
  private CheckpointManager.InstanceStateCheckpoint instanceCheckpointState;

  private HDFSStorage hdfsStorage;
  private FileSystem mockFileSystem;

  @Before
  public void before() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(StatefulStorageTestContext.ROOT_PATH_KEY, StatefulStorageTestContext.ROOT_PATH);

    mockFileSystem = mock(FileSystem.class);
    when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://hdfs_home_path/ckpgmgr"));
    when(mockFileSystem.getHomeDirectory()).thenReturn(new Path("hdfs_home_path"));

    PowerMockito.spy(FileSystem.class);
    PowerMockito.doReturn(mockFileSystem).when(FileSystem.class, "get", any(Configuration.class));

    hdfsStorage = spy(HDFSStorage.class);
    hdfsStorage.init(StatefulStorageTestContext.TOPOLOGY_NAME, config);

    instance = StatefulStorageTestContext.getInstance();
    instanceCheckpointState = StatefulStorageTestContext.getInstanceStateCheckpoint();
  }

  @After
  public void after() throws Exception {
    hdfsStorage.close();
  }

  @Test
  public void testStore() throws Exception {
    PowerMockito.mockStatic(CheckpointManager.InstanceStateCheckpoint.class);
    CheckpointManager.InstanceStateCheckpoint mockCheckpointState =
        mock(CheckpointManager.InstanceStateCheckpoint.class);

    Checkpoint checkpoint = new Checkpoint(mockCheckpointState);

    FSDataOutputStream mockFSDateOutputStream = mock(FSDataOutputStream.class);
    when(mockFileSystem.create(any(Path.class))).thenReturn(mockFSDateOutputStream);

    doNothing().when(hdfsStorage).createDir(anyString());

    final CheckpointInfo info = new CheckpointInfo(
        StatefulStorageTestContext.CHECKPOINT_ID, instance);
    hdfsStorage.storeCheckpoint(info, checkpoint);

    verify(mockCheckpointState).writeTo(mockFSDateOutputStream);
  }

  @Test
  public void testRestore() throws Exception {
    FSDataInputStream mockFSDataInputStream = mock(FSDataInputStream.class);

    when(mockFileSystem.open(any(Path.class))).thenReturn(mockFSDataInputStream);

    PowerMockito.spy(CheckpointManager.InstanceStateCheckpoint.class);
    PowerMockito.doReturn(instanceCheckpointState)
        .when(CheckpointManager.InstanceStateCheckpoint.class, "parseFrom",
            mockFSDataInputStream);

    final CheckpointInfo info = new CheckpointInfo(
        StatefulStorageTestContext.CHECKPOINT_ID, instance);
    Checkpoint restoreCheckpoint = hdfsStorage.restoreCheckpoint(info);
    assertEquals(restoreCheckpoint.getCheckpoint(), instanceCheckpointState);
  }

  @Test
  public void testDisposeAll() throws Exception {
    hdfsStorage.dispose(StatefulStorageTestContext.CHECKPOINT_ID, true);
    verify(mockFileSystem).delete(any(Path.class), eq(true));
  }

  @Test
  public void testDisposePartial() throws Exception {
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn("0");

    FileStatus mockFS1 = mock(FileStatus.class);
    when(mockFS1.getPath()).thenReturn(mockPath);

    FileStatus mockFS2 = mock(FileStatus.class);
    when(mockFS2.getPath()).thenReturn(mockPath);

    FileStatus[] mockFileStatus = {mockFS1, mockFS2};
    FileStatus[] emptyFileStatus = new FileStatus[0];
    when(mockFileSystem.listStatus(any(Path.class)))
        .thenReturn(mockFileStatus)
        .thenReturn(emptyFileStatus);

    hdfsStorage.dispose(StatefulStorageTestContext.CHECKPOINT_ID, false);

    verify(mockFileSystem, times(2)).delete(any(Path.class), eq(true));
  }
}
