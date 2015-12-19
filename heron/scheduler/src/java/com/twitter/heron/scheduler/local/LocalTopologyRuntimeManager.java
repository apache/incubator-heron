package com.twitter.heron.scheduler.local;

import com.twitter.heron.scheduler.api.IRuntimeManager;
import com.twitter.heron.scheduler.api.context.RuntimeManagerContext;

/**
 * Handles Runtime tasks like kill/restart/activate/deactivate for heron topology launched
 * in local scheduler.
 */
public class LocalTopologyRuntimeManager implements IRuntimeManager {

  @Override
  public void initialize(RuntimeManagerContext context) {
    // Safe-check, throws exception if particular config is not contained
    context.getPropertyWithException(LocalConfig.WORKING_DIRECTORY);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean prepareRestart(int containerIndex) {
    return true;
  }

  @Override
  public boolean postRestart(int containerIndex) {
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
