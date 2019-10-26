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

package org.apache.heron.scheduler.yarn;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public final class YarnContext extends Context {

  private YarnContext() {
  }

  public static String heronYarnQueue(Config cfg) {
    return cfg.getStringValue(YarnKey.HERON_SCHEDULER_YARN_QUEUE.value(),
        YarnKey.HERON_SCHEDULER_YARN_QUEUE.getDefaultString());
  }

  public static int heronDriverMemoryMb(Config cfg) {
    return cfg.getIntegerValue(YarnKey.YARN_SCHEDULER_DRIVER_MEMORY_MB.value(),
        YarnKey.YARN_SCHEDULER_DRIVER_MEMORY_MB.getDefaultInt());
  }
}
