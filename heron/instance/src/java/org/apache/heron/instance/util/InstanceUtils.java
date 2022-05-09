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

package org.apache.heron.instance.util;

import java.time.Duration;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.api.Pair;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;

public final class InstanceUtils {
  private InstanceUtils() {
  }

  @SuppressWarnings("unchecked")
  public static void prepareTimerEvents(ExecutorLooper looper, PhysicalPlanHelper helper) {
    Map<String, Pair<Duration, Runnable>> timerEvents =
        (Map<String, Pair<Duration, Runnable>>) helper.getTopologyContext()
            .getTopologyConfig().get(Config.TOPOLOGY_TIMER_EVENTS);

    if (timerEvents != null) {
      for (Map.Entry<String, Pair<Duration, Runnable>> entry : timerEvents.entrySet()) {
        Duration duration = entry.getValue().getFirst();
        Runnable task = entry.getValue().getSecond();

        looper.registerPeriodicEvent(duration, task);
      }
    }
  }
}
