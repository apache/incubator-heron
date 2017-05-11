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

package com.twitter.heron.statefulstorage.hdfs;


import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statefulstorage.Checkpoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, CheckpointManager.InstanceStateCheckpoint.class})
public class HDFSStorageTest {

  private static final String TOPOLOGY_NAME = "topology_name";
  private static final String ROOT_PATH_KEY = "root.path";
  private static final String ROOT_PATH = "HDFSTest";
  private static final String CHECKPOINT_ID = "checkpoint_id";
  private static final String STMGR_ID = "stmgr_id";
  private static final String INSTANCE_ID = "instance_id";
  private static final String COMPONENT_NAME = "component_name";
  private static final int TASK_ID = 1;
  private static final int COMPONENT_INDEX = 1;
  private static final byte[] BYTES = "HDFS test bytes".getBytes();

  private PhysicalPlans.Instance instance;
  private CheckpointManager.InstanceStateCheckpoint instanceCheckpointState;

  private HDFSStorage hdfsStorage;
  private FileSystem mockFileSystem;

  @Before
  public void before() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(ROOT_PATH_KEY, ROOT_PATH);

    mockFileSystem = mock(FileSystem.class);
    when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://hdfs_home_path/ckpgmgr"));
    when(mockFileSystem.getHomeDirectory()).thenReturn(new Path("hdfs_home_path"));

    PowerMockito.spy(FileSystem.class);
    PowerMockito.doReturn(mockFileSystem).when(FileSystem.class, "get", any(Configuration.class));

    hdfsStorage = spy(HDFSStorage.class);
    hdfsStorage.init(config);

    PhysicalPlans.InstanceInfo info = PhysicalPlans.InstanceInfo.newBuilder()
        .setTaskId(TASK_ID)
        .setComponentIndex(COMPONENT_INDEX)
        .setComponentName(COMPONENT_NAME)
        .build();

    instance = PhysicalPlans.Instance.newBuilder()
        .setInstanceId(INSTANCE_ID)
        .setStmgrId(STMGR_ID)
        .setInfo(info)
        .build();

    instanceCheckpointState = CheckpointManager.InstanceStateCheckpoint.newBuilder()
        .setCheckpointId(CHECKPOINT_ID)
        .setState(ByteString.copyFrom(BYTES))
        .build();
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

    Checkpoint checkpoint =
        new Checkpoint(TOPOLOGY_NAME, instance, mockCheckpointState);

    FSDataOutputStream mockFSDateOutputStream = mock(FSDataOutputStream.class);
    when(mockFileSystem.create(any(Path.class))).thenReturn(mockFSDateOutputStream);

    doReturn(true).when(hdfsStorage).createDirs(anyString());

    hdfsStorage.store(checkpoint);

    verify(mockCheckpointState).writeTo(mockFSDateOutputStream);
  }

  @Test
  public void testRestore() throws Exception {
    Checkpoint restoreCheckpoint = new Checkpoint(TOPOLOGY_NAME, instance, instanceCheckpointState);

    FSDataInputStream mockFSDataInputStream = mock(FSDataInputStream.class);

    when(mockFileSystem.open(any(Path.class))).thenReturn(mockFSDataInputStream);

    PowerMockito.spy(CheckpointManager.InstanceStateCheckpoint.class);
    PowerMockito.doReturn(instanceCheckpointState)
        .when(CheckpointManager.InstanceStateCheckpoint.class, "parseFrom", mockFSDataInputStream);

    hdfsStorage.restore(TOPOLOGY_NAME, CHECKPOINT_ID, instance);

    assertEquals(restoreCheckpoint.getCheckpoint(), instanceCheckpointState);
  }

  @Test
  public void testDisposeAll() throws Exception {
    hdfsStorage.dispose(TOPOLOGY_NAME, CHECKPOINT_ID, true);
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

    hdfsStorage.dispose(TOPOLOGY_NAME, CHECKPOINT_ID, false);

    verify(mockFileSystem, times(2)).delete(any(Path.class), eq(true));
  }
}
