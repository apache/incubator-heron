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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

final class Extractor {

  static void extract(InputStream in, Path destination) throws IOException {
    try (
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
        final GzipCompressorInputStream gzipInputStream =
            new GzipCompressorInputStream(bufferedInputStream);
        final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(gzipInputStream)
    ) {
      final String destinationAbsolutePath = destination.toFile().getAbsolutePath();

      TarArchiveEntry entry;
      while ((entry = (TarArchiveEntry) tarInputStream.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          File f = Paths.get(destinationAbsolutePath, entry.getName()).toFile();
          f.mkdirs();
        } else {
          Path fileDestinationPath = Paths.get(destinationAbsolutePath, entry.getName());

          Files.copy(tarInputStream, fileDestinationPath, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private Extractor() {
  }
}
