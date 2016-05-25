package com.twitter.heron.scheduler.mesos.framework.driver;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.logging.Logger;

public class MesosDriverFactory {
  private static final Logger LOG = Logger.getLogger(MesosDriverFactory.class.getName());

  SchedulerDriver mesosDriver;

  Scheduler scheduler;
  private final PersistenceStore persistenceStore;
  FrameworkConfiguration config;

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
      LOG.severe(String.format("MesosSchedulerDriver start resulted in status: %s. Committing suicide!", status));
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
      System.exit(1);
    }

    mesosDriver.stop(true);
    mesosDriver = null;
  }

  private SchedulerDriver makeDriver() {
    Protos.FrameworkID frameworkId = config.failoverTimeoutSeconds > 0 ?
        persistenceStore.getFrameworkID() : null;

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
