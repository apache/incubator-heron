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

package org.apache.heron.statemgr.zookeeper;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public final class ZkContext extends Context {
  public static final String IS_INITIALIZE_TREE = "heron.statemgr.zookeeper.is.initialize.tree";
  public static final String SESSION_TIMEOUT_MS = "heron.statemgr.zookeeper.session.timeout.ms";
  public static final String CONNECTION_TIMEOUT_MS =
      "heron.statemgr.zookeeper.connection.timeout.ms";
  public static final String RETRY_COUNT = "heron.statemgr.zookeeper.retry.count";
  public static final String RETRY_INTERVAL_MS = "heron.statemgr.zookeeper.retry.interval.ms";

  public static final String IS_TUNNEL_NEEDED = "heron.statemgr.is.tunnel.needed";

  private ZkContext() {
  }

  public static boolean isInitializeTree(Config config) {
    return config.getBooleanValue(IS_INITIALIZE_TREE, true);
  }

  public static int sessionTimeoutMs(Config config) {
    return config.getIntegerValue(SESSION_TIMEOUT_MS, 30000);
  }

  public static int connectionTimeoutMs(Config config) {
    return config.getIntegerValue(CONNECTION_TIMEOUT_MS, 30000);
  }

  public static int retryCount(Config config) {
    return config.getIntegerValue(RETRY_COUNT, 10);
  }

  public static int retryIntervalMs(Config config) {
    return config.getIntegerValue(RETRY_INTERVAL_MS, 10000);
  }
}
