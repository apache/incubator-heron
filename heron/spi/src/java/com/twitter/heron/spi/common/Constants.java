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

package com.twitter.heron.spi.common;

public final class Constants {

  // name of the resource file that holds the config keys
  public static final String KEYS_YAML = "com/twitter/heron/spi/common/keys.yaml";

  // name of the resource file that holds the default values for config keys
  public static final String DEFAULTS_YAML = "com/twitter/heron/spi/common/defaults.yaml";

  public static final long GB = 1024L * 1024 * 1024;
  public static final long MB = 1024L * 1024;

  public static final String TUNNEL = "tunnel";
  public static final String ZKHOST = "zkhost";
  public static final String ZKPORT = "zkport";
  public static final String ZKROOT = "zkroot";

  public static final String ZK_CONNECTION_STRING = "zk.connection.string";
  public static final String ZK_CONNECTION_TIMEOUT_MS = "zk.connection.timeout.ms";

  public static final String LOCALFS_STATE_MANAGER_CLASS =
      "com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager";

  public static final String ZK_STATE_MANAGER_CLASS =
      "com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager";

  private Constants() {
  }
}
