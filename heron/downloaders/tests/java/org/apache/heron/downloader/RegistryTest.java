//  Copyright 2017 Twitter. All rights reserved.
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
package org.apache.heron.downloader;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RegistryTest {

  @Test
  public void testGetDownloader() throws Exception {
    Map<String, Class<? extends Downloader>> downloaders = new HashMap<>();
    downloaders.put("http", HttpDownloader.class);
    downloaders.put("https", HttpDownloader.class);
    downloaders.put("distributedlog", DLDownloader.class);
    downloaders.put("file", FileDownloader.class);

    URI httpUri = URI.create("http://127.0.0.1/test/http");
    Downloader downloader = Registry.getDownloader(downloaders, httpUri);
    assertTrue(downloader instanceof HttpDownloader);
    URI httpsUri = URI.create("https://127.0.0.1/test/http");
    downloader = Registry.getDownloader(downloaders, httpsUri);
    assertTrue(downloader instanceof HttpDownloader);
    URI dlUri = URI.create("distributedlog://127.0.0.1/test/distributedlog");
    downloader = Registry.getDownloader(downloaders, dlUri);
    assertTrue(downloader instanceof DLDownloader);
  }

}
