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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.heron.spi.common.Config;

import static org.junit.Assert.assertTrue;

public class RegistryTest {

  @Test
  public void testGetDownloader() throws Exception {
    Map<String, String> protocols = new HashMap<>();
    protocols.put("http", "org.apache.heron.downloader.HttpDownloader");
    protocols.put("https", "org.apache.heron.downloader.HttpDownloader");
    protocols.put("distributedlog", "org.apache.heron.downloader.DLDownloader");
    protocols.put("file", "org.apache.heron.downloader.FileDownloader");
    Config config = Config.newBuilder()
        .put("heron.downloader.registry", protocols)
        .build();

    Map<String, Class<? extends Downloader>> downloaders = new HashMap<>();
    downloaders.put("http", HttpDownloader.class);
    downloaders.put("https", HttpDownloader.class);
    downloaders.put("distributedlog", DLDownloader.class);
    downloaders.put("file", FileDownloader.class);

    URI httpUri = URI.create("http://127.0.0.1/test/http");
    Class<? extends Downloader> clazz = Registry.UriToClass(config, httpUri);
    Downloader downloader = Registry.getDownloader(clazz, httpUri);
    assertTrue(downloader instanceof HttpDownloader);

    URI httpsUri = URI.create("https://127.0.0.1/test/http");
    clazz = Registry.UriToClass(config, httpsUri);
    downloader = Registry.getDownloader(clazz, httpsUri);
    assertTrue(downloader instanceof HttpDownloader);

    URI dlUri = URI.create("distributedlog://127.0.0.1/test/distributedlog");
    clazz = Registry.UriToClass(config, dlUri);
    downloader = Registry.getDownloader(clazz, dlUri);
    assertTrue(downloader instanceof DLDownloader);
  }

}
