package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.NetworkUtils;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles runtime tasks like kill/restart/activate/deactivate for 
 * heron topology launched in the local scheduler.
 *
 */
public class LocalRuntimeManager implements IRuntimeManager {

  private Config config;
  private Config runtime;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean prepareRestart() {
    return isTopologyRunning();
  }

  @Override
  public boolean postRestart() {
    return true;
  }

  @Override
  public boolean prepareDeactivate() {
    return isTopologyRunning();
  }

  @Override
  public boolean postDeactivate() {
    return true;
  }

  /**
   * Check for preconditions for activate such as 
   *   If the topology is running?
   *   If the topology is already in active state?
   *
   * @return true if the conditions are met
   */
  @Override
  public boolean prepareActivate() {
    return isTopologyRunning();
  }

  @Override
  public boolean postActivate() {
    return true;
  }

  /**
   * Check for preconditions for kill such as if the topology running?
   *
   * @return true if the conditions are met
   */
  @Override
  public boolean prepareKill() {
    return isTopologyRunning();
  }

  @Override
  public boolean postKill() {
    return true;
  }

  /**
   * Check if the topology is running from the state manager
   *
   * @return true if it is running, otherwise, false
   */
  protected boolean isTopologyRunning() {

    // get the topology name
    String topologyName = LocalContext.topologyName(config);

    // get the scheduler state manager
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);

    // issue the request to the state manager
    ListenableFuture<Boolean> boolFuture = statemgr.isTopologyRunning(topologyName);

    // wait until we get the result to check status
    if (!NetworkUtils.awaitResult(boolFuture, 1000, TimeUnit.MILLISECONDS)) {
      System.err.println("Topology " + topologyName + " is not running...");
      return false;
    }

    return true;
  }
}
