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

package org.apache.heron.statefulstorage.localfs;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.proto.ckptmgr.CheckpointManager.InstanceStateCheckpoint;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.statefulstorage.StatefulStorageTestContext;

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
public class LocalFileSystemStorageTest {
  private PhysicalPlans.Instance instance;
  private InstanceStateCheckpoint checkpoint;

  private LocalFileSystemStorage localFileSystemStorage;

  @Before
  public void before() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(StatefulStorageTestContext.ROOT_PATH_KEY, StatefulStorageTestContext.ROOT_PATH);

    localFileSystemStorage = spy(new LocalFileSystemStorage());
    localFileSystemStorage.init(config);

    instance = StatefulStorageTestContext.getInstance();
    checkpoint = StatefulStorageTestContext.getInstanceStateCheckpoint();
  }

  @After
  public void after() throws Exception {
    localFileSystemStorage.close();
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
    when(mockCheckpoint.getCheckpoint()).thenReturn(checkpoint);

    localFileSystemStorage.store(mockCheckpoint);

    PowerMockito.verifyStatic(times(1));
    FileUtils.writeToFile(anyString(), eq(checkpoint.toByteArray()), eq(true));
  }

  @Test
  public void testRestore() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(checkpoint.toByteArray())
        .when(FileUtils.class, "readFromFile", anyString());

    Checkpoint ckpt =
        new Checkpoint(StatefulStorageTestContext.TOPOLOGY_NAME, instance, checkpoint);

    localFileSystemStorage.restore(StatefulStorageTestContext.TOPOLOGY_NAME,
        StatefulStorageTestContext.CHECKPOINT_ID, instance);

    assertEquals(checkpoint, ckpt.getCheckpoint());
  }

  @Test
  public void testDispose() throws Exception {
    PowerMockito.spy(FileUtils.class);
    PowerMockito.doReturn(true).when(FileUtils.class, "deleteDir", anyString());
    PowerMockito.doReturn(false).when(FileUtils.class, "isDirectoryExists", anyString());

    localFileSystemStorage.dispose(StatefulStorageTestContext.TOPOLOGY_NAME, "", true);

    PowerMockito.verifyStatic(times(1));
    FileUtils.deleteDir(anyString());
    FileUtils.isDirectoryExists(anyString());
  }
}
