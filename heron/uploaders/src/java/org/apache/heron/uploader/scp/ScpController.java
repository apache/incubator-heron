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

package org.apache.heron.uploader.scp;

import java.util.logging.Logger;

import org.apache.heron.spi.utils.ShellUtils;

public class ScpController {
  private static final Logger LOG = Logger.getLogger(ScpController.class.getName());
  private String scpOptions;
  private String scpConnection;
  private String sshOptions;
  private String sshConnection;
  private boolean isVerbose;

  public ScpController(String scpOptions, String scpConnection, String sshOptions,
                       String sshConnection, boolean isVerbose) {
    this.scpOptions = scpOptions;
    this.scpConnection = scpConnection;

    this.sshOptions = sshOptions;
    this.sshConnection = sshConnection;

    this.isVerbose = isVerbose;
  }

  public boolean mkdirsIfNotExists(String dir) {
    // an example ssh command created by the format looks like this:
    // ssh -i ~/.ssh/id_rsa -p 23 user@example.com mkdir -p /heron/repository/...
    String command = String.format("ssh %s %s mkdir -p %s", sshOptions, sshConnection, dir);
    return 0 == ShellUtils.runProcess(command, null);
  }

  public boolean copyFromLocalFile(String source, String destination) {
    // an example scp command created by the format looks like this:
    // scp -i ~/.ssh/id_rsa -p 23 ./foo.tar.gz user@example.com:/heron/foo.tar.gz
    String command =
        String.format("scp %s %s %s:%s", scpOptions, source, scpConnection, destination);
    return 0 == ShellUtils.runProcess(command, null);
  }

  public boolean delete(String filePath) {
    String command = String.format("ssh %s %s rm -rf %s", sshOptions, sshConnection, filePath);
    return 0 == ShellUtils.runProcess(command, null);
  }
}
