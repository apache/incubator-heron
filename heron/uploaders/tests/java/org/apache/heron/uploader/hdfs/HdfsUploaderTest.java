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

package org.apache.heron.uploader.hdfs;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.uploader.UploaderException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class HdfsUploaderTest {
  private HdfsUploader uploader;
  private HdfsController controller;

  @Before
  public void setUp() throws Exception {
    Config config = mock(Config.class);

    // Insert mock HdfsController
    uploader = spy(new HdfsUploader());
    controller = mock(HdfsController.class);
    doReturn(controller).when(uploader).getHdfsController();

    uploader.initialize(config);
  }

  @After
  public void after() throws Exception {
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageLocalFileNotExist() throws Exception {
    doReturn(false).when(uploader).isLocalFileExists(anyString());
    uploader.uploadPackage();
    verify(controller, never()).copyFromLocalFile(anyString(), anyString());
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCreateFolderOnHDFS() throws Exception {
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    doReturn(false).when(controller).exists(anyString());
    doReturn(false).when(controller).mkdirs(anyString());
    uploader.uploadPackage();
    verify(controller, never()).copyFromLocalFile(anyString(), anyString());
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCopyFromLocalToHDFS() throws Exception {
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    doReturn(true).when(controller).mkdirs(anyString());
    doReturn(false).when(controller).copyFromLocalFile(anyString(), anyString());
    uploader.uploadPackage();
    verify(controller).copyFromLocalFile(anyString(), anyString());
  }

  @Test
  public void testUploadPackage() {
    // Happy path
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    doReturn(true).when(controller).mkdirs(anyString());
    doReturn(true).when(controller).copyFromLocalFile(anyString(), anyString());
    uploader.uploadPackage();
    verify(controller, atLeastOnce()).copyFromLocalFile(anyString(), anyString());
  }

  @Test
  public void testUndo() throws Exception {
    doReturn(false).when(controller).delete(anyString());
    assertFalse(uploader.undo());
    verify(controller).delete(anyString());

    doReturn(true).when(controller).delete(anyString());
    assertTrue(uploader.undo());
  }
}
