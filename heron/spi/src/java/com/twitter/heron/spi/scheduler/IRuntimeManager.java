package com.twitter.heron.spi.scheduler;

import com.twitter.heron.spi.common.Config;

public interface IRuntimeManager {

  enum Command {
    KILL,
    ACTIVATE,
    DEACTIVATE,
    RESTART;
  }

  void initialize(Config config);

  void close();

  boolean prepareRestart();

  boolean postRestart();

  boolean prepareDeactivate();

  boolean postDeactivate();

  boolean prepareActivate();

  boolean postActivate();

  boolean prepareKill();

  boolean postKill();
}
