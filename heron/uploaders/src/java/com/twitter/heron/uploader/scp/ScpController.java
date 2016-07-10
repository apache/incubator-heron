// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.uploader.scp;

import java.util.logging.Logger;

import com.twitter.heron.spi.utils.ShellUtils;

public class ScpController {
  private static final Logger LOG = Logger.getLogger(ScpController.class.getName());
  private String scpCommand;
  private String sshCommand;
  private boolean isVerbose;

  public ScpController(String scpCommand, String sshCommand, boolean isVerbose) {
    this.scpCommand = scpCommand;
    this.sshCommand = sshCommand;
    this.isVerbose = isVerbose;
  }

  public boolean mkdirsIfNotExists(String dir) {
    String command = String.format("ssh %s mkdir -p %s", sshCommand,  dir);
    return 0 == ShellUtils.runProcess(isVerbose, command, null, null);
  }

  public boolean copyFromLocalFile(String source, String destination) {
    String command = String.format("scp %s %s:%s", source, scpCommand, destination);
    return 0 == ShellUtils.runProcess(isVerbose, command, null, null);
  }

  public boolean delete(String filePath) {
    String command = String.format("ssh %s rm -rf %s", sshCommand, filePath);
    return 0 == ShellUtils.runProcess(isVerbose, command, null, null);
  }
}
