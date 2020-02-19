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

package org.apache.heron.downloader;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.heron.dlog.DLInputStream;

import static org.apache.heron.downloader.DLDownloader.CONF;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(Extractor.class)
public class DLDownloaderTest {

  @Test
  public void testOpenInputStream() throws Exception {
    Namespace ns = mock(Namespace.class);
    DistributedLogManager dlm = mock(DistributedLogManager.class);
    LogReader reader = mock(LogReader.class);
    when(ns.openLog(anyString())).thenReturn(dlm);
    when(dlm.getInputStream(eq(DLSN.InitialDLSN))).thenReturn(reader);

    InputStream is = DLDownloader.openInputStream(ns, "test-open-inputstream");
    assertTrue(is instanceof DLInputStream);

    is.close();
    verify(dlm, times(1)).close();
    verify(reader, times(1)).close();
  }

  @Test
  public void testDownload() throws Exception {
    String logName = "test-download";
    URI uri = URI.create("distributedlog://127.0.0.1/test/distributedlog/" + logName);

    File tempFile = File.createTempFile("test", "download");
    // make sure it is deleted when the test completes
    tempFile.deleteOnExit();
    Path path = Paths.get(tempFile.toURI());

    Namespace ns = mock(Namespace.class);
    DistributedLogManager dlm = mock(DistributedLogManager.class);
    LogReader reader = mock(LogReader.class);
    when(ns.openLog(anyString())).thenReturn(dlm);
    when(dlm.getInputStream(eq(DLSN.InitialDLSN))).thenReturn(reader);
    when(reader.readNext(anyBoolean())).thenThrow(new EndOfStreamException("eos"));

    NamespaceBuilder nsBuilder = mock(NamespaceBuilder.class);
    when(nsBuilder.clientId(anyString())).thenReturn(nsBuilder);
    when(nsBuilder.conf(any(DistributedLogConfiguration.class))).thenReturn(nsBuilder);
    when(nsBuilder.uri(any(URI.class))).thenReturn(nsBuilder);
    when(nsBuilder.build()).thenReturn(ns);

    PowerMockito.mockStatic(Extractor.class);
    PowerMockito.doNothing()
        .when(Extractor.class, "extract", any(InputStream.class), any(Path.class));

    DLDownloader downloader = new DLDownloader(() -> nsBuilder);
    downloader.download(uri, path);

    URI parentUri = URI.create("distributedlog://127.0.0.1/test/distributedlog");
    verify(nsBuilder, times(1)).clientId(eq("heron-downloader"));
    verify(nsBuilder, times(1)).conf(eq(CONF));
    verify(nsBuilder, times(1)).uri(parentUri);

    PowerMockito.verifyStatic(times(1));
    Extractor.extract(any(InputStream.class), eq(path));

    verify(ns, times(1)).openLog(eq(logName));
    verify(ns, times(1)).close();
  }

}
