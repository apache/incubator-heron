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

package org.apache.heron.apiserver.actions;

import org.apache.heron.scheduler.RuntimeManagerRunner;

public final class Keys {

  public static final String PARAM_COMPONENT_PARALLELISM =
      RuntimeManagerRunner.RUNTIME_MANAGER_COMPONENT_PARALLELISM_KEY;
  public static final String PARAM_CONTAINER_NUMBER =
      RuntimeManagerRunner.RUNTIME_MANAGER_CONTAINER_NUMBER_KEY;
  public static final String PARAM_USER_RUNTIME_CONFIG =
      RuntimeManagerRunner.RUNTIME_MANAGER_RUNTIME_CONFIG_KEY;

  private Keys() {
  }
}
