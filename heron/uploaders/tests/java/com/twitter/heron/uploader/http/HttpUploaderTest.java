//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.uploader.http;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.uploader.UploaderException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpUploaderTest extends LocalServerTestBase {

  private static final String EXPECTED_URI = "/test";

  private HttpHost httpHost;
  private Config config;
  private static File tempFile;

  @BeforeClass
  public static void setUpClass() throws IOException {
    tempFile = File.createTempFile("test_topology", ".tar.gz");
  }

  @AfterClass
  public static void tearDownClass() {
    tempFile.delete();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.serverBootstrap.registerHandler(
        "/*", (HttpRequestHandler) (request, response, context) -> {
          response.setStatusCode(HttpStatus.SC_OK);
          response.setEntity(new StringEntity(EXPECTED_URI));
        });

    httpHost = start();

    final URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(httpHost.getHostName())
        .setPort(httpHost.getPort())
        .setPath(EXPECTED_URI)
        .build();

    // Create the minimum config for tests
    config = Config.newBuilder()
        .put(Key.CLUSTER, "cluster")
        .put(Key.ROLE, "role")
        .put(Key.TOPOLOGY_NAME, "topology")
        .put(Key.TOPOLOGY_PACKAGE_TYPE, PackageType.TAR)
        .put(Key.TOPOLOGY_PACKAGE_FILE, tempFile.getCanonicalPath())
        .put(HttpUploaderContext.HERON_UPLOADER_HTTP_URI, uri.getPath())
        .build();
  }

  @After
  public void shutdown() throws Exception {
    if (this.httpclient != null) {
      this.httpclient.close();
    }
    if (this.server != null) {
      this.server.shutdown(0L, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testUndo() {
    HttpUploader httpUploader = new TestHttpUploader();
    assertFalse(httpUploader.undo());
  }

  @Test
  public void testUploadPackage() {
    HttpUploader httpUploader = new TestHttpUploader();
    httpUploader.initialize(config);
    URI uri = httpUploader.uploadPackage();
    assertTrue(uri.getPath().equals(EXPECTED_URI));
  }

  @Test(expected = UploaderException.class)
  public void testUploadPackageWhenUploaderExceptionIsThrown() {
    HttpUploader httpUploader = new TestHttpUploaderWithException();
    httpUploader.initialize(config);
    httpUploader.uploadPackage();
  }

  private class TestHttpUploader extends HttpUploader {
    @Override
    protected HttpResponse execute(HttpClient client) throws IOException {
      HttpResponse httpResponse = client.execute(httpHost, getPost());
      assertEquals(HttpStatus.SC_OK, httpResponse.getStatusLine().getStatusCode());
      return httpResponse;
    }
  }

  private class TestHttpUploaderWithException extends HttpUploader {
    @Override
    protected HttpResponse execute(HttpClient client) throws IOException {
      throw new IOException("Topology package can not be uploaded");
    }
  }
}
