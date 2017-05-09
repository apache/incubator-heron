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

package com.twitter.heron.statefulstorage.localfs;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.ckptmgr.CheckpointManager.GetInstanceStateRequest;
import com.twitter.heron.proto.ckptmgr.CheckpointManager.InstanceStateCheckpoint;
import com.twitter.heron.proto.ckptmgr.CheckpointManager.SaveInstanceStateRequest;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statefulstorage.Checkpoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class LocalFSStorageTest {

  private static final String TOPOLOGY_NAME = "topology_name";
  private static final String ROOT_PATH_KEY = "root.path";
  private static final String ROOT_PATH = "localFSTest";
  private static final String CHECKPOINT_ID = "checkpoint_id";
  private static final String STMGR_ID = "stmgr_id";
  private static final String INSTANCE_ID = "instance_id";
  private static final String COMPONENT_NAME = "component_name";
  private static final int TASK_ID = 1;
  private static final int COMPONENT_INDEX = 1;
  private static final byte[] BYTES = "LocalFS test bytes".getBytes();
  private LocalFSStorage localFileSystemBackend;
  private SaveInstanceStateRequest saveInstanceStateRequest;
  private GetInstanceStateRequest getInstanceStateRequest;
  private PhysicalPlans.Instance instance;
  private InstanceStateCheckpoint checkpoint;

  @Before
  public void before() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(ROOT_PATH_KEY, ROOT_PATH);

    localFileSystemBackend = spy(new LocalFSStorage());
    localFileSystemBackend.init(config);

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

    checkpoint = InstanceStateCheckpoint.newBuilder()
        .setCheckpointId(CHECKPOINT_ID)
        .setState(ByteString.copyFrom(BYTES))
        .build();

    saveInstanceStateRequest = SaveInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpoint(checkpoint)
        .build();

    getInstanceStateRequest = GetInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpointId(CHECKPOINT_ID)
        .build();
  }

  @After
  public void after() throws Exception {
    localFileSystemBackend.close();
  }

  @Test
  public void testStore() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).when(FileUtils.class, "createDirectory", anyString());
    PowerMockito.doReturn(true).when(FileUtils.class, "isFileExists", anyString());
    PowerMockito.doReturn(true).when(FileUtils.class, "isDirectoryExists", anyString());
    PowerMockito.doReturn(true)
        .when(FileUtils.class, "writeToFile", anyString(), any(byte[].class), anyBoolean());

    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    when(mockCheckpoint.checkpoint()).thenReturn(saveInstanceStateRequest);

    localFileSystemBackend.store(mockCheckpoint);

    PowerMockito.verifyStatic(times(1));
    FileUtils.writeToFile(anyString(), eq(saveInstanceStateRequest.toByteArray()), eq(true));
  }

  @Test
  public void testRestore() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(saveInstanceStateRequest.toByteArray())
        .when(FileUtils.class, "readFromFile", anyString());

    Checkpoint ckpt = new Checkpoint(TOPOLOGY_NAME, getInstanceStateRequest);

    localFileSystemBackend.restore(ckpt);

    assertEquals(saveInstanceStateRequest, ckpt.checkpoint());
  }

  @Test
  public void testDispose() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).when(FileUtils.class, "deleteDir", anyString());
    PowerMockito.doReturn(false).when(FileUtils.class, "isDirectoryExists", anyString());

    localFileSystemBackend.dispose(TOPOLOGY_NAME, "", true);

    PowerMockito.verifyStatic(times(1));
    FileUtils.deleteDir(anyString());
    FileUtils.isDirectoryExists(anyString());
  }
}
