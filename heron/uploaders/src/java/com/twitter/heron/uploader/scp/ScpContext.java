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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class ScpContext extends Context {
  public static final String HERON_UPLOADER_SCP_COMMAND = "heron.uploader.scp.command.options";
  public static final String HERON_UPLOADER_SSH_COMMAND = "heron.uploader.ssh.command.options";
  public static final String HERON_UPLOADER_SCP_DIR_PATH = "heron.uploader.scp.dir.path";
  public static final String HERON_UPLOADER_SCP_DIR_PATH_DEFAULT =
      "${HOME}/heron/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}";

  public static String scpCommand(Config config) {
    return config.getStringValue(HERON_UPLOADER_SCP_COMMAND);
  }

  public static String sshCommand(Config config) {
    return config.getStringValue(HERON_UPLOADER_SSH_COMMAND);
  }

  public static String uploadDirPath(Config config) {
    return config.getStringValue(HERON_UPLOADER_SCP_DIR_PATH, HERON_UPLOADER_SCP_DIR_PATH_DEFAULT);
  }
}
