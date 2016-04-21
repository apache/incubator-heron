package com.twitter.heron.spi.common;

/***
 * This enum defines Command invoked from heron client
 */
public enum Command {
  // TODO(mfu): Move ACTIVATE & DEACTIVATE out? They are non-related to Scheduling
  SUBMIT,
  KILL,
  ACTIVATE,
  DEACTIVATE,
  RESTART;

  public static Command makeCommand(String commandString) {
    return Command.valueOf(commandString.toUpperCase());
  }
}