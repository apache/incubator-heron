/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.yarn;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.client.FailedJob;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.client.RunningJob;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

/**
 * Contains client-side listeners for YARN scheduler events emitted by REEF.
 */
@Unit
public class ReefClientSideHandlers {
  private static final Logger LOG = Logger.getLogger(ReefClientSideHandlers.class.getName());
  private final String topologyName;
  private CountDownLatch jobStatusWatcher = new CountDownLatch(1);
  // Volatile for thread safety
  private volatile boolean result;

  @Inject
  public ReefClientSideHandlers(
      @Parameter(HeronConfigurationOptions.TopologyName.class) String topologyName) {
    LOG.log(Level.INFO, "Initializing REEF client handlers for Heron, topology: {0}", topologyName);
    this.topologyName = topologyName;
  }

  /**
   * Wait indefinitely to receive events from driver
   */
  public boolean waitForSchedulerJobResponse() throws InterruptedException {
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
      LOG.log(Level.INFO,
          "Topology {0} is running, jobId {1}.",
          new Object[]{topologyName, job.getId()});
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
