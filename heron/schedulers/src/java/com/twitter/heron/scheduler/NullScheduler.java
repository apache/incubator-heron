package com.twitter.heron.spi.scheduler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

public class NullScheduler implements IScheduler {
  @Override
  public void initialize(LaunchContext context) {
  }

  @Override
  public void schedule(PackingPlan packing) {
  }

  @Override
  public void onHealthCheck(String healthCheckResponse) {
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

