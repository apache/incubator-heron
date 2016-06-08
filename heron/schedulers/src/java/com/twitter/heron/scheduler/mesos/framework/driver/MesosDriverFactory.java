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

package com.twitter.heron.scheduler.mesos.framework.driver;

import java.util.logging.Logger;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;

public class MesosDriverFactory {
  private static final Logger LOG = Logger.getLogger(MesosDriverFactory.class.getName());

  private SchedulerDriver mesosDriver;

  private Scheduler scheduler;
  private final PersistenceStore persistenceStore;
  private FrameworkConfiguration config;

  public MesosDriverFactory(Scheduler scheduler,
                            PersistenceStore persistenceStore,
                            FrameworkConfiguration config) {
    this.scheduler = scheduler;
    this.persistenceStore = persistenceStore;
    this.config = config;
  }

  public void start() {
    Protos.Status status = get().start();
    if (status != Protos.Status.DRIVER_RUNNING) {
      LOG.severe(String.format("MesosSchedulerDriver start resulted in status: %s. "
          + "Committing suicide!", status));

      // CHECKSTYLE:OFF RegexpSinglelineJava
      System.exit(1);
    }
  }

  public void join() {
    get().join();
  }

  public SchedulerDriver get() {
    if (mesosDriver == null) {
      LOG.info("SchedulerDriver not exists yet. To create a new one...");
      mesosDriver = makeDriver();
    }

    return mesosDriver;
  }

  public void close() {
    if (mesosDriver == null) {
      LOG.severe("Attempted to close a non-initialed driver");

      // CHECKSTYLE:OFF RegexpSinglelineJava
      System.exit(1);
    }

    mesosDriver.stop(true);
    mesosDriver = null;
  }

  private SchedulerDriver makeDriver() {
    Protos.FrameworkID frameworkId = config.failoverTimeoutSeconds > 0
        ? persistenceStore.getFrameworkID() : null;

    if (frameworkId == null) {
      LOG.info("No pre-existing FrameworkID. First time to start the scheduler.");
    }

    return SchedulerDriverBuilder.newBuilder().
        setFrameworkInfo(config, frameworkId).
        setScheduler(scheduler).
        setCredentials(config.authenticationPrincipal, config.authenticationSecretFile).
        build();
  }
}
