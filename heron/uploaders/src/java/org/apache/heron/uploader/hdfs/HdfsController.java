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

package org.apache.heron.uploader.hdfs;

import org.apache.heron.spi.utils.ShellUtils;

public class HdfsController {
  private final String configDir;
  private final boolean isVerbose;

  public HdfsController(String configDir, boolean isVerbose) {
    this.configDir = configDir;
    this.isVerbose = isVerbose;
  }

  public boolean exists(String filePath) {
    String command = String.format("hadoop --config %s fs -test -e %s", configDir, filePath);
    return 0 == ShellUtils.runProcess(command, null);
  }

  public boolean mkdirs(String dir) {
    String command = String.format("hadoop --config %s fs -mkdir -p %s", configDir, dir);
    return 0 == ShellUtils.runProcess(command, null);
  }

  public boolean copyFromLocalFile(String source, String destination) {
    String command = String.format("hadoop --config %s fs -copyFromLocal -f %s %s",
        configDir, source, destination);
    return 0 == ShellUtils.runProcess(command, null);
  }

  public boolean delete(String filePath) {
    String command = String.format("hadoop --config %s fs -rm %s", configDir, filePath);
    return 0 == ShellUtils.runProcess(command, null);
  }
}
