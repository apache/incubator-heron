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

package com.twitter.heron.statemgr.zookeeper;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.SpiCommonConfig;

public final class ZkContext extends Context {
  public static final String IS_INITIALIZE_TREE = "heron.statemgr.zookeeper.is.initialize.tree";
  public static final String SESSION_TIMEOUT_MS = "heron.statemgr.zookeeper.session.timeout.ms";
  public static final String CONNECTION_TIMEOUT_MS =
      "heron.statemgr.zookeeper.connection.timeout.ms";
  public static final String RETRY_COUNT = "heron.statemgr.zookeeper.retry.count";
  public static final String RETRY_INTERVAL_MS = "heron.statemgr.zookeeper.retry.interval.ms";

  public static final String IS_TUNNEL_NEEDED = "heron.statemgr.is.tunnel.needed";
  public static final String TUNNEL_CONNECTION_TIMEOUT_MS =
      "heron.statemgr.tunnel.connection.timeout.ms";
  public static final String TUNNEL_CONNECTION_RETRY_COUNT =
      "heron.statemgr.tunnel.connection.retry.count";
  public static final String TUNNEL_VERIFY_COUNT = "heron.statemgr.tunnel.verify.count";
  public static final String TUNNEL_RETRY_INTERVAL_MS = "heron.statemgr.tunnel.retry.interval.ms";
  public static final String TUNNEL_HOST = "heron.statemgr.tunnel.host";

  private ZkContext() {
  }

  public static boolean isInitializeTree(SpiCommonConfig config) {
    return config.getBooleanValue(IS_INITIALIZE_TREE, true);
  }

  public static int sessionTimeoutMs(SpiCommonConfig config) {
    return config.getIntegerValue(SESSION_TIMEOUT_MS, 30000);
  }

  public static int connectionTimeoutMs(SpiCommonConfig config) {
    return config.getIntegerValue(CONNECTION_TIMEOUT_MS, 30000);
  }

  public static int retryCount(SpiCommonConfig config) {
    return config.getIntegerValue(RETRY_COUNT, 10);
  }

  public static int retryIntervalMs(SpiCommonConfig config) {
    return config.getIntegerValue(RETRY_INTERVAL_MS, 10000);
  }

  ///////////////////////////////////////////////////////
  // Following are config for tunneling
  ///////////////////////////////////////////////////////

  public static boolean isTunnelNeeded(SpiCommonConfig config) {
    return config.getBooleanValue(IS_TUNNEL_NEEDED, false);
  }

  public static int tunnelConnectionTimeoutMs(SpiCommonConfig config) {
    return config.getIntegerValue(TUNNEL_CONNECTION_TIMEOUT_MS, 1000);
  }

  public static int tunnelConnectionRetryCount(SpiCommonConfig config) {
    return config.getIntegerValue(TUNNEL_CONNECTION_RETRY_COUNT, 2);
  }

  public static int tunnelVerifyCount(SpiCommonConfig config) {
    return config.getIntegerValue(TUNNEL_VERIFY_COUNT, 10);
  }

  public static int tunnelRetryIntervalMs(SpiCommonConfig config) {
    return config.getIntegerValue(TUNNEL_RETRY_INTERVAL_MS, 1000);
  }

  public static String tunnelHost(SpiCommonConfig config) {
    return config.getStringValue(TUNNEL_HOST, "no.tunnel.host.specified");
  }
}
