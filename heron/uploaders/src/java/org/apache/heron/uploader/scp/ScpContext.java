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

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public class ScpContext extends Context {
  public static final String HERON_UPLOADER_SCP_OPTIONS = "heron.uploader.scp.command.options";
  public static final String HERON_UPLOADER_SCP_CONNECTION =
      "heron.uploader.scp.command.connection";
  public static final String HERON_UPLOADER_SSH_OPTIONS = "heron.uploader.ssh.command.options";
  public static final String HERON_UPLOADER_SSH_CONNECTION =
      "heron.uploader.ssh.command.connection";
  public static final String HERON_UPLOADER_SCP_DIR_PATH = "heron.uploader.scp.dir.path";
  public static final String HERON_UPLOADER_SCP_DIR_PATH_DEFAULT =
      "${HOME}/heron/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}";

  public static String scpOptions(Config config) {
    return config.getStringValue(HERON_UPLOADER_SCP_OPTIONS);
  }

  public static String scpConnection(Config config) {
    return config.getStringValue(HERON_UPLOADER_SCP_CONNECTION);
  }

  public static String sshOptions(Config config) {
    return config.getStringValue(HERON_UPLOADER_SSH_OPTIONS);
  }

  public static String sshConnection(Config config) {
    return config.getStringValue(HERON_UPLOADER_SSH_CONNECTION);
  }

  public static String uploadDirPath(Config config) {
    return config.getStringValue(HERON_UPLOADER_SCP_DIR_PATH, HERON_UPLOADER_SCP_DIR_PATH_DEFAULT);
  }
}
