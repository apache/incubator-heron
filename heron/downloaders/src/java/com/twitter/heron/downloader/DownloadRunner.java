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
package com.twitter.heron.downloader;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class DownloadRunner {

  // takes topology package URI and extracts it to a directory
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: downloader <topology-package-uri> <extract-destination>");
      return;
    }

    final String uri = args[0];
    final String destination = args[1];

    final URI topologyLocation = new URI(uri);
    final Path topologyDestination = Paths.get(destination);

    final File file = topologyDestination.toFile();
    if (!file.exists()) {
      file.mkdirs();
    }

    final Downloader downloader = Registry.get().getDownloader(topologyLocation);
    downloader.download(topologyLocation, topologyDestination);
  }

  private DownloadRunner() {
  }
}
