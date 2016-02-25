package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IRuntimeManager;

/**
 * Handles runtime tasks like kill/restart/activate/deactivate for heron 
 * topology launched in the local scheduler.
 */
public class LocalRuntimeManager implements IRuntimeManager {

  @Override
  public void initialize(Config config) {
  }

  @Override
  public void close() {
  }

  @Override
  public boolean prepareRestart() {
    return true;
  }

  @Override
  public boolean postRestart() {
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

  @Override
  public boolean prepareActivate() {
    return true;
  }

  @Override
  public boolean postActivate() {
    return true;
  }

  @Override
  public boolean prepareKill() {
    return true;
  }

  @Override
  public boolean postKill() {
    return true;
  }
}
