//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.instance.util;

import java.time.Duration;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.Pair;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

public final class InstanceUtils {
  private InstanceUtils() {
  }

  @SuppressWarnings("unchecked")
  public static void prepareTimerEvents(SlaveLooper looper, PhysicalPlanHelper helper) {
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
