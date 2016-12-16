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

package com.twitter.heron.scheduler.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.SchedulerException;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class SchedulerClientFactory {
  private static final Logger LOG = Logger.getLogger(SchedulerClientFactory.class.getName());

  private Config config;

  private Config runtime;

  public SchedulerClientFactory(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
  }

  /**
   * Implementation of getSchedulerClient - Used to create objects
   * Currently it creates either HttpServiceSchedulerClient or LibrarySchedulerClient
   *
   * @return getSchedulerClient created. return null if failed to create ISchedulerClient instance
   */
  public ISchedulerClient getSchedulerClient() throws SchedulerException {
    LOG.fine("Creating scheduler client");
    ISchedulerClient schedulerClient;

    if (Context.schedulerService(config)) {
      // get the instance of the state manager
      SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);

      Scheduler.SchedulerLocation schedulerLocation =
          statemgr.getSchedulerLocation(Runtime.topologyName(runtime));

      if (schedulerLocation == null) {
        throw new SchedulerException("Failed to get scheduler location from state manager");
      }

      LOG.log(Level.FINE, "Scheduler is listening on location: {0} ", schedulerLocation.toString());

      schedulerClient =
          new HttpServiceSchedulerClient(config, runtime, schedulerLocation.getHttpEndpoint());
    } else {
      // create an instance of scheduler
      final IScheduler scheduler = LauncherUtils.getInstance()
          .getSchedulerInstance(config, runtime);

      LOG.fine("Invoke scheduler as a library");
      schedulerClient = new LibrarySchedulerClient(config, runtime, scheduler);
    }

    return schedulerClient;
  }
}
