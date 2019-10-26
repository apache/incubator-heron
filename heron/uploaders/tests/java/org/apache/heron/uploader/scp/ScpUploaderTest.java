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

package org.apache.heron.uploader.scp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.uploader.UploaderException;

public class ScpUploaderTest {
  private Config config;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    config = Mockito.mock(Config.class);
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testConfiguration() throws Exception {
    // Insert mock ScpUploader
    ScpUploader uploader = Mockito.spy(new ScpUploader());

    // exception
    Mockito.doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SCP_OPTIONS);
    exception.expect(RuntimeException.class);
    uploader.getScpController();

    // exception
    Mockito.doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SSH_OPTIONS);
    exception.expect(RuntimeException.class);
    uploader.getScpController();
    // exception
    Mockito.doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SCP_CONNECTION);
    exception.expect(RuntimeException.class);
    uploader.getScpController();
    // exception
    Mockito.doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SSH_CONNECTION);
    exception.expect(RuntimeException.class);
    uploader.getScpController();

    // happy path
    Mockito.doReturn(Mockito.anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SCP_OPTIONS);
    Mockito.doReturn(Mockito.anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SCP_CONNECTION);
    Mockito.doReturn(Mockito.anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SSH_OPTIONS);
    Mockito.doReturn(Mockito.anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SSH_CONNECTION);
    Assert.assertNotNull(uploader.getScpController());
  }

  // Local file not exist
  @Test(expected = UploaderException.class)
  public void testUploadPackageLocalFileDoesNotExist() throws Exception {
    ScpUploader uploader = Mockito.spy(new ScpUploader());
    ScpController controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);
    Mockito.doReturn(false).when(uploader).isLocalFileExists(Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  // Failed to create folder on remote
  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCreateRemoteFolder() throws Exception {
    ScpUploader uploader = Mockito.spy(new ScpUploader());
    ScpController controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).mkdirsIfNotExists(Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller, Mockito.never()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  // Failed to copy file from local to remote
  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCopyFromLocalToRemote() throws Exception {
    ScpUploader uploader = Mockito.spy(new ScpUploader());
    ScpController controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    uploader.initialize(config);
    Mockito.doReturn(true).when(controller).mkdirsIfNotExists(Mockito.anyString());
    Mockito.doReturn(false).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller).copyFromLocalFile(Mockito.anyString(), Mockito.anyString());
  }

  // Happy path
  @Test
  public void testUploadPackage() {
    ScpUploader uploader = Mockito.spy(new ScpUploader());
    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());
    ScpController controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();
    Mockito.doReturn(true).when(controller).mkdirsIfNotExists(Mockito.anyString());
    uploader.initialize(config);
    Mockito.doReturn(true).when(controller).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
    uploader.uploadPackage();
    Mockito.verify(controller, Mockito.atLeastOnce()).copyFromLocalFile(
        Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testUndo() throws Exception {
    ScpUploader uploader = Mockito.spy(new ScpUploader());
    ScpController controller = Mockito.mock(ScpController.class);
    Mockito.doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);

    Mockito.doReturn(false).when(controller).delete(Mockito.anyString());
    Assert.assertFalse(uploader.undo());
    Mockito.verify(controller).delete(Mockito.anyString());

    Mockito.doReturn(true).when(controller).delete(Mockito.anyString());
    Assert.assertTrue(uploader.undo());
  }
}
