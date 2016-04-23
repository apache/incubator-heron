package com.twitter.heron.scheduler.reef;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.client.FailedJob;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.client.RunningJob;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

/**
 * Contains client-side listeners for REEF scheduler events.
 */
@Unit
public class ReefClientSideHandlers {
  private static final Logger LOG = Logger.getLogger(ReefClientSideHandlers.class.getName());

  private CountDownLatch jobStatusWatcher = new CountDownLatch(1);
  private String topologyName;

  // Volatile for thread safety
  private volatile boolean result;

  @Inject
  public ReefClientSideHandlers() {
  }

  public void initialize(String topologyName) {
    this.topologyName = topologyName;
  }

  /**
   * Wait indefinitely to receive events from driver
   */
  public boolean waitForJobToStart() throws InterruptedException {
    result = false;
    jobStatusWatcher.await();
    return result;
  }

  /**
   * Job driver notifies us that the job is running.
   */
  public final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.INFO, "Topology {0} is running, jobId {1}.", new Object[]{topologyName, job.getId()});
      result = true;
      jobStatusWatcher.countDown();
    }
  }

  /**
   * Handle topology driver failure event
   */
  public final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOG.log(Level.SEVERE, "Failed to start topology: " + topologyName, job.getReason());
      result = false;
      jobStatusWatcher.countDown();
    }
  }

  /**
   * Handle an error in the in starting driver for topology.
   */
  public final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Failed to start topology: " + topologyName, error.getReason());
      result = false;
      jobStatusWatcher.countDown();
    }
  }
}
