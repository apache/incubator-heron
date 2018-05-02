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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RegistryTest {

  @Test
  public void testGetDownloader() throws Exception {
    URI httpUri = URI.create("http://127.0.0.1/test/http");
    Downloader downloader = Registry.get().getDownloader(httpUri);
    assertTrue(downloader instanceof HttpDownloader);
    URI httpsUri = URI.create("https://127.0.0.1/test/http");
    downloader = Registry.get().getDownloader(httpsUri);
    assertTrue(downloader instanceof HttpDownloader);
    URI dlUri = URI.create("distributedlog://127.0.0.1/test/distributedlog");
    downloader = Registry.get().getDownloader(dlUri);
    assertTrue(downloader instanceof DLDownloader);
  }

}
