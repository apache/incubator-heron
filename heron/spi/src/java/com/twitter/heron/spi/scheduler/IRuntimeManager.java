package com.twitter.heron.spi.scheduler;

import com.twitter.heron.spi.common.Config;

public interface IRuntimeManager extends AutoCloseable {
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

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the RuntimeManager
   * <p/>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
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
