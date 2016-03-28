package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IRuntimeManager;

/**
 * Handles runtime tasks like kill/restart/activate/deactivate for
 * heron topology launched in the local scheduler.
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
  public boolean prepareRestart(Integer containerId) {
    return true;
  }

  @Override
  public boolean postRestart(Integer containerId) {
    return true;
  }

  @Override
  public boolean prepareDeactivate() {
    return true;
  }

  @Override
  public boolean postDeactivate() {
    return true;
  }

  /**
   * Check for preconditions for activate such as
   * If the topology is running?
   * If the topology is already in active state?
   *
   * @return true if the conditions are met
   */
  @Override
  public boolean prepareActivate() {
    return true;
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
    return true;
  }

  @Override
  public boolean postKill() {
    return true;
  }
}
