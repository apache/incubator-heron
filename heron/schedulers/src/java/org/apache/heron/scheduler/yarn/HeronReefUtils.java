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

package org.apache.heron.scheduler.yarn;

import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.spi.utils.ShellUtils;

final class HeronReefUtils {
  private static final Logger LOG = Logger.getLogger(HeronReefUtils.class.getName());

  /**
   * This is a utility class and should not be instantiated.
   */
  private HeronReefUtils() {
  }

  static void extractPackageInSandbox(String srcFolder, String fileName, String dstDir) {
    String packagePath = Paths.get(srcFolder, fileName).toString();
    LOG.log(Level.INFO, "Extracting package: {0} at: {1}", new Object[]{packagePath, dstDir});
    boolean result = ShellUtils.extractPackage(packagePath, dstDir, false, true);
    if (!result) {
      String msg = "Failed to extract package:" + packagePath + " at: " + dstDir;
      throw new RuntimeException(msg);
    }
  }
}
