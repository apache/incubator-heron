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

package com.twitter.heron.spi.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class SchedulerUtils {
  private static final Logger LOG = Logger.getLogger(SchedulerUtils.class.getName());

  private SchedulerUtils() {
    // Throw an exception if this ever *is* called
    throw new AssertionError("Instantiating utility class " + this.getClass().getSimpleName());
  }

  /**
   * Invoke the onScheduler() in IScheduler directly as a library
   *
   * @param config The Config to initialize IScheduler
   * @param runtime The runtime Config to initialize IScheduler
   * @param scheduler the IScheduler to invoke
   * @param packing The PackingPlan to scheduler for OnSchedule()
   * @return true if scheduling successfully
   */
  public static boolean onScheduleAsLibrary(Config config, Config runtime,
      IScheduler scheduler, PackingPlan packing) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onSchedule(packing);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to invoke schedule method", e);
      return false;
    } finally {
      scheduler.close();
    }

    return ret;
  }


  /**
   * Clean all states of a heron topology
   * 1. Topology def and ExecutionState are required to exist to delete
   * 2. TMasterLocation, SchedulerLocation and PhysicalPlan may not exist to delete
   */
  public static boolean cleanState(String topologyName, SchedulerStateManagerAdaptor statemgr) {
    LOG.fine("Cleaning up Heron State");

    Boolean result;

    result = statemgr.deleteTopology(topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to clear topology state");
      return false;
    }

    result = statemgr.deleteExecutionState(topologyName);
    if (result == null || !result) {
      LOG.severe("Failed to clear execution state");
      return false;
    }

    // It is possible that  TMasterLocation, PhysicalPlan and SchedulerLocation are not set
    // Just log but don't consider them failure
    result = statemgr.deleteTMasterLocation(topologyName);
    if (result == null || !result) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.warning("Failed to clear TMaster location. Check whether TMaster set it correctly.");
    }

    result = statemgr.deletePhysicalPlan(topologyName);
    if (result == null || !result) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.warning("Failed to clear physical plan. Check whether TMaster set it correctly.");
    }

    result = statemgr.deleteSchedulerLocation(topologyName);
    if (result == null || !result) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.warning("Failed to clear scheduler location. Check whether Scheduler set it correctly.");
    }

    LOG.fine("Cleaned up Heron State");
    return true;
  }
}
