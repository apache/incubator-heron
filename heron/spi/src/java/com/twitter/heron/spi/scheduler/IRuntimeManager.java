package com.twitter.heron.spi.scheduler;

import com.twitter.heron.spi.common.Config;

public interface IRuntimeManager {
  enum Command {
    KILL,
    ACTIVATE,
    DEACTIVATE,
    RESTART;

    public static Command makeCommand(String commandString) {
      return Command.valueOf(commandString.toUpperCase());
    }
  }

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
