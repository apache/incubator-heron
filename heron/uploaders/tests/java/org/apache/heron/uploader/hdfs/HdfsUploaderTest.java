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

package org.apache.heron.uploader.hdfs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.uploader.UploaderException;

public class HdfsUploaderTest {
  private HdfsUploader uploader;
  private HdfsController controller;

  @Before
  public void setUp() throws Exception {
    Config config = Mockito.mock(Config.class);

    // Insert mock HdfsController
    uploader = Mockito.spy(new HdfsUploader());
    controller = Mockito.mock(HdfsController.class);
    Mockito.doReturn(controller).when(uploader).getHdfsController();

    uploader.initialize(config);
  }

  @After
  public void after() throws Exception {
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageLocalFileNotExist() throws Exception {
    Mockito.doReturn(false).when(uploader).isLocalFileExists(Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCreateFolderOnHDFS() throws Exception {
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).exists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).mkdirs(Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCopyFromLocalToHDFS() throws Exception {
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    Mockito.doReturn(true).when(controller).mkdirs(Mockito.anyString());
    Mockito.doReturn(false).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller).copyFromLocalFile(Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testUploadPackage() {
    // Happy path
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    Mockito.doReturn(true).when(controller).mkdirs(Mockito.anyString());
    Mockito.doReturn(true).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    uploader.uploadPackage();
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
