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

package org.apache.heron.apiserver;

import org.apache.heron.spi.common.Key;

public final class Constants {

  static final int DEFAULT_PORT = 9000;

  static final String DEFAULT_HERON_LOCAL = "~/.heron";

  static final String DEFAULT_HERON_CONFIG_DIRECTORY = "conf";

  static final String DEFAULT_HERON_RELEASE_FILE = "release.yaml";

  public static final String DEFAULT_HERON_SANDBOX_CONFIG =
      Key.HERON_CLUSTER_CONF.getDefaultString();

  public static final String OVERRIDE_FILE = "override.yaml";

  public static final String STATE_MANAGER_FILE = "statemgr.yaml";

  public static final String DEFAULT_HERON_ENVIRONMENT = "default";

  private Constants() {
  }
}
