package com.twitter.heron.spi.scheduler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;

public class NullScheduler implements IScheduler {

  @Override
  public void initialize(Config config, Config runtime) {

  }

  @Override
  public void close() {

  }

  @Override
  public void schedule(PackingPlan packing) {

  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    return true;
  }

  @Override
  public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
    return true;
  }

  @Override
  public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return true;
  }
}

