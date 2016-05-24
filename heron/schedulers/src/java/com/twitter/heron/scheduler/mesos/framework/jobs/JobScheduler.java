package com.twitter.heron.scheduler.mesos.framework.jobs;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosDriverFactory;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosJobFramework;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;

import java.util.Map;
import java.util.logging.Logger;

public class JobScheduler {
  private static final Logger LOG = Logger.getLogger(JobScheduler.class.getName());

  private final MesosJobFramework mesosJobFramework;

  private final PersistenceStore persistenceStore;
  private final MesosDriverFactory mesosDriver;
  private final FrameworkConfiguration config;

  public JobScheduler(MesosJobFramework mesosJobFramework,
                      PersistenceStore persistenceStore,
                      MesosDriverFactory mesosDriver,
                      FrameworkConfiguration config) {
    this.mesosJobFramework = mesosJobFramework;
    this.persistenceStore = persistenceStore;
    this.mesosDriver = mesosDriver;
    this.config = config;
  }

  public void clear() {
    mesosJobFramework.clear();

    // Clear the frameworkId
    persistenceStore.removeFrameworkID();
  }

  public void updateJob(BaseJob olbJob, BaseJob newJob) {
    throw new RuntimeException("Not yet implemented");
  }

  public boolean isReady() {
    return mesosJobFramework.isReady();
  }

  public boolean registerJob(BaseJob job) {
    if (!mesosJobFramework.isReady()) {
      LOG.info("The Scheduler is not ready; recovering from last failure. Please wait.");
      return false;
    }

    // WAL
    LOG.info("Persist Job. WAL the job: " + job.name);
    persistenceStore.persistJob(job);

    return mesosJobFramework.scheduleNewJob(job);
  }

  public boolean restartJob(String jobName) {
    if (!mesosJobFramework.isReady()) {
      LOG.info("The Scheduler is not ready; recovering from last failure. Please wait.");
      return false;
    }

    return mesosJobFramework.restartScheduledJob(jobName, mesosDriver);
  }

  public boolean deregisterJob(String jobName, boolean killJobNow) {
    if (!mesosJobFramework.isReady()) {
      LOG.info("The Scheduler is not ready; recovering from last failure. Please wait.");
      return false;
    }

    return mesosJobFramework.removeScheduledJob(jobName, mesosDriver, killJobNow);
  }

  /***
   * What we need to do:
   * 1. Recover all states in MesosJobFramework, including jobNameMapping, runningTasks and taskQueue
   * -- return to the consistent states
   * 2. Reconcile to the mesos master to know what the latest state should be and apply the changes
   * -- Send reconcile periodically until we have the latest status of all task
   * -- Before we have the latest status of all task, we would
   * 3. Relief the flag, which could enable submit/kill/deploy tasks in taskQueue.
   */
  public void start() {
    LOG.info("Starting the Job Scheduler!");

    Iterable<BaseJob> jobs = persistenceStore.getJobs();
    mesosJobFramework.loadJobs(jobs);
    LOG.info("Loaded all jobs from last failure");

    Map<String, BaseTask> tasks = persistenceStore.getTasks();
    mesosJobFramework.loadTasks(tasks);
    LOG.info("Loaded all tasks from last failure");

    mesosJobFramework.loadPendingScheduleTasks();
    LOG.info("Loaded all pendingSchedule tasks");

    LOG.info("To start the MesosDriver");
    mesosDriver.start();
  }

  public void join() {
    mesosDriver.join();
  }

  public void stop() {
    LOG.info("Shutting down the Job Scheduler!");
    mesosDriver.close();

    LOG.info("Clearing the states.....");
    clear();
  }
}
