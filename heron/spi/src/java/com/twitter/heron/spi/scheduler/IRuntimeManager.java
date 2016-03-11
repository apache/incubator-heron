package com.twitter.heron.spi.scheduler;

import com.twitter.heron.spi.common.Config;

public interface IRuntimeManager {

  void initialize(Config config, Config runtime);

  void close();

  boolean prepareRestart(Integer containerId);

  boolean postRestart(Integer containerId);

  boolean prepareDeactivate();

  boolean postDeactivate();

  boolean prepareActivate();

  boolean postActivate();

  boolean prepareKill();

  boolean postKill();
}
