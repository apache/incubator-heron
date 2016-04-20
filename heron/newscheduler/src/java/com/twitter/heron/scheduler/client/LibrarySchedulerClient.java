package com.twitter.heron.scheduler.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IScheduler;

/**
 * This class manages topology by invoking IScheduler's interface directly as a library.
 */
public class LibrarySchedulerClient implements ISchedulerClient {
  private static final Logger LOG = Logger.getLogger(LibrarySchedulerClient.class.getName());

  private final Config config;
  private final Config runtime;
  private final IScheduler scheduler;

  public LibrarySchedulerClient(Config config, Config runtime, IScheduler scheduler) {
    this.config = config;
    this.runtime = runtime;
    this.scheduler = scheduler;
  }

  @Override
  public boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onRestart(restartTopologyRequest);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to restart topology", e);
      return false;
    } finally {
      scheduler.close();
    }

    return ret;
  }

  @Override
  public boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onKill(killTopologyRequest);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to kill topology", e);
      return false;
    } finally {
      scheduler.close();
    }

    return ret;
  }

  // TODO(mfu): Use JAVA8's lambda feature providing a method for all commands in SchedulerUtils
  // TODO(mfu): boolean invokeSchedulerAsLibrary(String commandName, Function invoker);
}
