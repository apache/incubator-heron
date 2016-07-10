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

package com.twitter.heron.uploader.scp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.common.Config;

public class ScpUploaderTest {
  private ScpUploader uploader;
  private ScpController controller;

  @Before
  public void setUp() throws Exception {
    Config config = Mockito.mock(Config.class);

    // Insert mock HdfsController
    uploader = Mockito.spy(new ScpUploader());
    controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();

    uploader.initialize(config);
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testUploadPackage() throws Exception {
    // Local file not exist
    Mockito.doReturn(false).when(uploader).isLocalFileExists(Mockito.anyString());
    Assert.assertNull(uploader.uploadPackage());
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());

    // Failed to create folder on remote
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).mkdirsIfNotExists(Mockito.anyString());
    Assert.assertNull(uploader.uploadPackage());
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());

    // Failed to copy file from local to remote
    Mockito.doReturn(true).when(controller).mkdirsIfNotExists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    Assert.assertNull(uploader.uploadPackage());
    Mockito.verify(controller).copyFromLocalFile(Mockito.anyString(), Mockito.anyString());

    // Happy path
    Mockito.doReturn(true).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    Assert.assertNotNull(uploader.uploadPackage());
    Mockito.verify(controller, Mockito.atLeastOnce()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testUndo() throws Exception {
    Mockito.doReturn(false).when(controller).delete(Mockito.anyString());
    Assert.assertFalse(uploader.undo());
    Mockito.verify(controller).delete(Mockito.anyString());

    Mockito.doReturn(true).when(controller).delete(Mockito.anyString());
    Assert.assertTrue(uploader.undo());
  }
}
