package com.twitter.heron.command;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class CommandHandlerFactory {
  private static final Logger LOG = Logger.getLogger(CommandHandlerFactory.class.getName());

  public static CommandHandler makeCommand(String command, Config config, Config runtime) {
    if (command.equalsIgnoreCase("ps"))
      return new ListTopologiesHandler(config, runtime);

    LOG.info("Invalid command " + command);
    return null;
  }
}
