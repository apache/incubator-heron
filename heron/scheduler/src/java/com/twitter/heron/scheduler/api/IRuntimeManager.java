package com.twitter.heron.scheduler.api;

import com.twitter.heron.scheduler.api.context.RuntimeManagerContext;

public interface IRuntimeManager {

  enum Command {
    KILL,
    ACTIVATE,
    DEACTIVATE,
    RESTART;
  }

  void initialize(RuntimeManagerContext context);

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
