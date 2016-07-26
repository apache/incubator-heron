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
package com.twitter.heron.scheduler.common;

import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.SchedulerUtils;

/**
 * {@link LauncherUtils} contains helper methods used by the server and client side launch
 * controllers; {@link com.twitter.heron.scheduler.LaunchRunner} and
 * {@link com.twitter.heron.scheduler.SchedulerMain}
 */
public class LauncherUtils {
  private static final Logger LOG = Logger.getLogger(LauncherUtils.class.getName());

  /**
   * Invoke the onScheduler() in IScheduler directly as a library
   *
   * @param config The Config to initialize IScheduler
   * @param runtime The runtime Config to initialize IScheduler
   * @param scheduler the IScheduler to invoke
   * @param packing The PackingPlan to scheduler for OnSchedule()
   * @return true if scheduling successfully
   */
  public static boolean onScheduleAsLibrary(
      Config config,
      Config runtime,
      IScheduler scheduler,
      PackingPlan packing) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onSchedule(packing);

      if (ret) {
        // Set the SchedulerLocation at last step,
        // since some methods in IScheduler will provide correct values
        // only after IScheduler.onSchedule is invoked correctly
        ret = SchedulerUtils.setLibSchedulerLocation(runtime, scheduler, false);
      } else {
        LOG.severe("Failed to invoke IScheduler as library");
      }
    } finally {
      scheduler.close();
    }

    return ret;
  }
}
