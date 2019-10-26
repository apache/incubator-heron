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

package org.apache.heron.uploader.dlog;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.uploader.UploaderException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

public class DlogUploaderTest {

  private static final String DL_URI = "distributedlog://127.0.0.1/test/uploader";

  private Config config;
  private DLUploader uploader;
  private NamespaceBuilder nsBuilder;
  private Copier copier;

  @Before
  public void setUp() throws Exception {
    config = mock(Config.class);
    when(config.getStringValue(eq(DLContext.DL_TOPOLOGIES_NS_URI)))
        .thenReturn(DL_URI);
    nsBuilder = mock(NamespaceBuilder.class);
    when(nsBuilder.clientId(anyString())).thenReturn(nsBuilder);
    when(nsBuilder.conf(any(DistributedLogConfiguration.class))).thenReturn(nsBuilder);
    when(nsBuilder.uri(any(URI.class))).thenReturn(nsBuilder);

    copier = mock(Copier.class);
    uploader = new DLUploader(() -> nsBuilder, copier);
  }

  @Test(expected = RuntimeException.class)
  public void testInitializeFailure() throws Exception {
    IOException ioe = new IOException("test-initialization");
    when(nsBuilder.build()).thenThrow(ioe);

    uploader.initialize(config);
  }

  @Test
  public void testInitialize() throws Exception {
    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);

    uploader.initialize(config);

    assertEquals(config, uploader.getConfig());
    assertEquals(DL_URI, uploader.getDestTopologyNamespaceURI());
    assertEquals(Context.topologyPackageFile(config), uploader.getTopologyPackageLocation());
    assertEquals(
        URI.create(String.format("%s/%s", DL_URI, uploader.getPackageName())),
        uploader.getPackageURI());
  }

  @Test
  public void testUndoSuccess() throws Exception {
    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);

    uploader.initialize(config);
    assertTrue(uploader.undo());

    verify(ns, times(1)).deleteLog(eq(uploader.getPackageName()));
  }

  @Test
  public void testUndoFailure() throws Exception {
    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);
    Mockito.doThrow(new IOException("test")).when(ns).deleteLog(anyString());

    uploader.initialize(config);
    assertFalse(uploader.undo());

    verify(ns, times(1)).deleteLog(eq(uploader.getPackageName()));
  }

  @Test
  public void testClose() throws Exception {
    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);

    uploader.initialize(config);
    uploader.close();

    verify(ns, times(1)).close();
  }

  @Test
  public void testUploadPackageLocalFileNotExist() throws Exception {
    uploader = Mockito.spy(uploader);

    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);
    Mockito.doReturn(false).when(uploader).isLocalFileExists(Mockito.anyString());

    uploader.initialize(config);
    try {
      uploader.uploadPackage();
      fail("Should fail on uploading package");
    } catch (UploaderException ue) {
      // expected
    }
    verify(ns, never()).logExists(anyString());
  }

  @Test
  public void testUploadPackage() throws Exception {
    uploader = Mockito.spy(uploader);

    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);
    when(ns.logExists(anyString())).thenReturn(false);
    DistributedLogManager dlm = mock(DistributedLogManager.class);
    when(ns.openLog(anyString())).thenReturn(dlm);
    AppendOnlyStreamWriter asw = mock(AppendOnlyStreamWriter.class);
    when(dlm.getAppendOnlyStreamWriter()).thenReturn(asw);

    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());

    uploader.initialize(config);
    uploader.uploadPackage();

    verify(ns, never()).deleteLog(eq(uploader.getPackageName()));
    verify(copier, times(1))
        .copyFileToStream(eq(uploader.getTopologyPackageLocation()), any(OutputStream.class));
    verify(asw, times(1)).close();
    verify(dlm, times(1)).close();
  }

  @Test
  public void testUploadPackageExisting() throws Exception {
    uploader = Mockito.spy(uploader);

    Namespace ns = mock(Namespace.class);
    when(nsBuilder.build()).thenReturn(ns);
    when(ns.logExists(anyString())).thenReturn(true);
    DistributedLogManager dlm = mock(DistributedLogManager.class);
    when(ns.openLog(anyString())).thenReturn(dlm);
    AppendOnlyStreamWriter asw = mock(AppendOnlyStreamWriter.class);
    when(dlm.getAppendOnlyStreamWriter()).thenReturn(asw);

    Mockito.doReturn(true).when(uploader).isLocalFileExists(Mockito.anyString());

    uploader.initialize(config);
    uploader.uploadPackage();

    verify(ns, times(1)).deleteLog(eq(uploader.getPackageName()));
    verify(copier, times(1))
        .copyFileToStream(eq(uploader.getTopologyPackageLocation()), any(OutputStream.class));
    verify(asw, times(1)).close();
    verify(dlm, times(1)).close();
  }

}
