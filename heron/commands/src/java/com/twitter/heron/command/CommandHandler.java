package com.twitter.heron.command;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public abstract class CommandHandler {

  // static config read from the config files
  protected Config config;

  // runtime config gathered during execution
  protected Config runtime;

  /**
   * Construct the command handler with static and runtime config
   */
  CommandHandler(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
  }

  /**
   * Execute any conditions before the command execution
   */
  public abstract boolean beforeExecution() throws Exception;

  /**
   * Execute any cleanup after the command execution
   */
  public abstract boolean afterExecution() throws Exception;

  /** 
   * Execute the command
   */
  public abstract boolean execute() throws Exception;
}
