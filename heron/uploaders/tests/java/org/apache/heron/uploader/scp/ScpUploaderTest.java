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

package org.apache.heron.uploader.scp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.uploader.UploaderException;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class ScpUploaderTest {
  private Config config;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    config = mock(Config.class);
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testConfiguration() throws Exception {
    // Insert mock ScpUploader
    ScpUploader uploader = spy(new ScpUploader());

    // exception
    doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SCP_OPTIONS);
    exception.expect(RuntimeException.class);
    uploader.getScpController();

    // exception
    doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SSH_OPTIONS);
    exception.expect(RuntimeException.class);
    uploader.getScpController();
    // exception
    doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SCP_CONNECTION);
    exception.expect(RuntimeException.class);
    uploader.getScpController();
    // exception
    doReturn(null).when(config).getStringValue(ScpContext.HERON_UPLOADER_SSH_CONNECTION);
    exception.expect(RuntimeException.class);
    uploader.getScpController();

    // happy path
    doReturn(anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SCP_OPTIONS);
    doReturn(anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SCP_CONNECTION);
    doReturn(anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SSH_OPTIONS);
    doReturn(anyString()).when(config).getStringValue(
        ScpContext.HERON_UPLOADER_SSH_CONNECTION);
    Assert.assertNotNull(uploader.getScpController());
  }

  // Local file not exist
  @Test(expected = UploaderException.class)
  public void testUploadPackageLocalFileDoesNotExist() throws Exception {
    ScpUploader uploader = spy(new ScpUploader());
    ScpController controller = mock(ScpController.class);
    doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);
    doReturn(false).when(uploader).isLocalFileExists(anyString());
    uploader.uploadPackage();
    verify(controller, never()).copyFromLocalFile(anyString(), anyString());
  }

  // Failed to create folder on remote
  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCreateRemoteFolder() throws Exception {
    ScpUploader uploader = spy(new ScpUploader());
    ScpController controller = mock(ScpController.class);
    doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    doReturn(false).when(controller).mkdirsIfNotExists(anyString());
    uploader.uploadPackage();
    verify(controller, never()).copyFromLocalFile(anyString(), anyString());
  }

  // Failed to copy file from local to remote
  @Test(expected = UploaderException.class)
  public void testUploadPackageFailToCopyFromLocalToRemote() throws Exception {
    ScpUploader uploader = spy(new ScpUploader());
    ScpController controller = mock(ScpController.class);
    doReturn(controller).when(uploader).getScpController();
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    uploader.initialize(config);
    doReturn(true).when(controller).mkdirsIfNotExists(anyString());
    doReturn(false).when(controller).copyFromLocalFile(anyString(), anyString());
    uploader.uploadPackage();
    verify(controller).copyFromLocalFile(anyString(), anyString());
  }

  // Happy path
  @Test
  public void testUploadPackage() {
    ScpUploader uploader = spy(new ScpUploader());
    doReturn(true).when(uploader).isLocalFileExists(anyString());
    ScpController controller = mock(ScpController.class);
    doReturn(controller).when(uploader).getScpController();
    doReturn(true).when(controller).mkdirsIfNotExists(anyString());
    uploader.initialize(config);
    doReturn(true).when(controller).copyFromLocalFile(anyString(), anyString());
    uploader.uploadPackage();
    verify(controller, atLeastOnce()).copyFromLocalFile(anyString(), anyString());
  }

  @Test
  public void testUndo() throws Exception {
    ScpUploader uploader = spy(new ScpUploader());
    ScpController controller = mock(ScpController.class);
    doReturn(controller).when(uploader).getScpController();
    uploader.initialize(config);

    doReturn(false).when(controller).delete(anyString());
    Assert.assertFalse(uploader.undo());
    verify(controller).delete(anyString());

    doReturn(true).when(controller).delete(anyString());
    Assert.assertTrue(uploader.undo());
  }
}
