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

package org.apache.heron.spi.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility used by Uploader
 */
public final class UploaderUtils {
  public static final String DEFAULT_FILENAME_EXTENSION = ".tar.gz";

  private UploaderUtils() {

  }

  /**
   * Generate a unique filename to upload in the storage service
   *
   * @param topologyName topology name
   * @param role role owns the topology
   * @return a unique filename
   */
  public static String generateFilename(
      String topologyName,
      String role) {
    // By default, we have empty tag info and version 0
    return generateFilename(topologyName, role, "tag", 0, DEFAULT_FILENAME_EXTENSION);
  }

  /**
   * Generate a unique filename to upload in the storage service
   *
   * @param topologyName topology name
   * @param role role owns the topology
   * @param version version of the job, put 0 if not needed
   * @param tag extra info to tag the file
   * @param extension file extension
   * @return a unique filename
   */
  public static String generateFilename(
      String topologyName,
      String role,
      String tag,
      int version,
      String extension) {
    return String.format("%s-%s-%s-%d%s",
        topologyName, role, tag, version, extension);
  }

  public static void copyToOutputStream(String inFile,
                                        OutputStream out) throws IOException {
    try (InputStream in = new FileInputStream(inFile)) {
      int read = 0;
      byte[] bytes = new byte[128 * 1024];
      while ((read = in.read(bytes)) >= 0) {
        if (0 == read) {
          continue;
        }
        out.write(bytes, 0, read);
      }
      out.flush();
      out.close();
    }
  }

}
