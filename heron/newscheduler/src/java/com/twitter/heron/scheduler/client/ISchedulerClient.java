package com.twitter.heron.scheduler.client;

import com.twitter.heron.proto.scheduler.Scheduler;

/**
 * Implementing this interface allows an object to manage topology via scheduler
 */
public interface ISchedulerClient {
  /**
   * Restart a topology on given RestartTopologyRequest
   *
   * @param restartTopologyRequest info for restart command
   * @return true if restarted successfully
   */
  boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest);

  /**
   * Kill a topology on given KillTopologyRequest
   *
   * @param killTopologyRequest info for kill command
   * @return true if killed successfully
   */
  boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest);
}
