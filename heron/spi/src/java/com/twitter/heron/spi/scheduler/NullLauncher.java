package com.twitter.heron.spi.scheduler;

import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;

public class NullLauncher implements ILauncher {

  @Override
  public void initialize(Config config, Config runtime) {

  }

  @Override
  public void close() {

  }

  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public boolean launch(PackingPlan packing) {
    return true;
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(ExecutionEnvironment.ExecutionState executionState) {
    return executionState;
  }
}

