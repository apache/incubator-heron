package com.twitter.heron.scheduler.service;

import com.twitter.heron.scheduler.api.IScheduler;
import com.twitter.heron.scheduler.util.NetworkUtility;

import java.util.concurrent.TimeUnit;

/**
 * Performs health check on scheduler.
 */
public class HealthCheckRunner implements Runnable {
  private final IScheduler scheduler;

  public HealthCheckRunner(IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void run() {
    // TODO(nbhagat): Implement health-check to executor.
    NetworkUtility.await(1, TimeUnit.MILLISECONDS);
  }
}