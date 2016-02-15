package com.twitter.heron.spi.scheduler;

import com.twitter.heron.spi.common.Context;

public interface IRuntimeManager {

  enum Command {
    KILL,
    ACTIVATE,
    DEACTIVATE,
    RESTART;
  }

  void initialize(Context context);

  void close();

  boolean prepareRestart(int containerIndex);

  boolean postRestart(int containerIndex);

  boolean prepareDeactivate();

  boolean postDeactivate();

  boolean prepareActivate();

  boolean postActivate();

  boolean prepareKill();

  boolean postKill();
}
