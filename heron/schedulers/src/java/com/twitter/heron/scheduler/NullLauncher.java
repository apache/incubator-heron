package com.twitter.heron.spi.scheduler;

import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

public class NullLauncher implements ILauncher {
  @Override
  public void initialize(LaunchContext context) {
  }

  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public boolean launchTopology(PackingPlan packing) {
    return true;
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public void undo() {
  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    return executionState;
  }
}

