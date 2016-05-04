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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class SchedulerUtils {
  private static final Logger LOG = Logger.getLogger(SchedulerUtils.class.getName());

  private SchedulerUtils() {
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
  public static boolean onScheduleAsLibrary(
      Config config,
      Config runtime,
      IScheduler scheduler,
      PackingPlan packing) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onSchedule(packing);

      // Set the SchedulerLocation at last step,
      // since some methods in IScheduler will provide correct values
      // only after IScheduler.onSchedule is invoked correctly
      ret = ret && setSchedulerLocation(runtime, scheduler);
    } finally {
      scheduler.close();
    }

    return ret;
  }

  public static String[] schedulerCommand(Config config, String javaBinary, int httpPort) {
    String schedulerClassPath = new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();

    List<String> commands = new ArrayList<>();
    commands.add(javaBinary);
    commands.add("-cp");
    commands.add(schedulerClassPath);
    commands.add("com.twitter.heron.scheduler.SchedulerMain");
    commands.add("--cluster");
    commands.add(Context.cluster(config));
    commands.add("--role");
    commands.add(Context.role(config));
    commands.add("--environment");
    commands.add(Context.environ(config));
    commands.add("--topology_name");
    commands.add(Context.topologyName(config));
    commands.add("--topology_jar");
    commands.add(Context.topologyJarFile(config));
    commands.add("--http_port");
    commands.add(Integer.toString(httpPort));

    return commands.toArray(new String[0]);
  }

  /**
   * Set the location of scheduler for other processes to discover,
   * when invoke IScheduler as a library on client side
   *
   * @param runtime, the runtime configuration
   * @param scheduler, the IScheduler to provide more info
   */
  public static boolean setSchedulerLocation(
      Config runtime,
      IScheduler scheduler) {
    // Dummy values since there is no running scheduler server
    final String serverHost = "scheduling_as_library";
    final int serverPort = -1;

    return setSchedulerLocation(runtime, serverHost, serverPort, scheduler);
  }

  /**
   * Set the location of scheduler for other processes to discover
   *
   * @param runtime, the runtime configuration
   * @param schedulerServerHost, the http server host that scheduler listens for receives requests
   * @param schedulerServerPort, the http server port that scheduler listens for receives requests
   * @param scheduler, the IScheduler to provide more info
   */
  public static boolean setSchedulerLocation(
      Config runtime,
      String schedulerServerHost,
      int schedulerServerPort,
      IScheduler scheduler) {

    // Set scheduler location to host:port by default. Overwrite scheduler location if behind DNS.
    Scheduler.SchedulerLocation.Builder builder = Scheduler.SchedulerLocation.newBuilder()
        .setTopologyName(Runtime.topologyName(runtime))
        .setHttpEndpoint(
            String.format("%s:%d", schedulerServerHost, schedulerServerPort));

    // Set the job link in SchedulerLocation if any
    String jobLink = scheduler.getJobLink();
    // Check whether IScheduler provides valid job link
    if (jobLink != null && !jobLink.equals("")) {
      builder.setJobPageLink(jobLink);
    }

    Scheduler.SchedulerLocation location = builder.build();

    LOG.log(Level.INFO, "Setting SchedulerLocation: {0}", location);
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    Boolean result = statemgr.setSchedulerLocation(location, Runtime.topologyName(runtime));

    if (result == null || !result) {
      LOG.severe("Failed to set Scheduler location");
      return false;
    }

    return true;
  }
}
