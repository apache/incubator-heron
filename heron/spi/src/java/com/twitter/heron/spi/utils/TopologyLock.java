package com.twitter.heron.spi.utils;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

/**
 * A TopologyLock utils class
 * It is required to acquire the TopologyLock before submitting a topology.
 * In two cases, a TopologyLock will be released:
 * 1. The topology is killed
 * 2. The topology submission failed
 *
 * The TopologyLock guarantees that with the same name only one topology can run
 * at the same time.
 */
public class TopologyLock {
  private static final Logger LOG = Logger.getLogger(TopologyUtils.class.getName());

  // TODO(mfu): Should it be a pure utils class or a normal object can be instantiated?
  // TODO(mfu): The lifecycle of this lock is from the submission to death of a topology,
  // TODO(mfu): rather than just in one phrase. Prefer not to instantiate it.
  private TopologyLock() {

  }

  /**
   * Acquire the topology lock by trying to store the trimmed topology definition into the state manager
   * Make sure this method is invoked before doing any operations on submitting a topology.
   * No more submissions are allowed after topology lock has been acquired.
   * This lock is to guarantee no two topologies with the same name can run at the same time.
   *
   * @param statemgr the state manager to persist the lock
   * @param topology the info stored in the lock
   * <p/>
   * * @return true if acquired lock successfully
   */
  public static boolean acquire(SchedulerStateManagerAdaptor statemgr, TopologyAPI.Topology topology) {
    ListenableFuture<Boolean> booleanFuture;
    Boolean futureResult;

    booleanFuture =
        statemgr.setTopology(TopologyUtils.trimTopology(topology), topology.getName());
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      LOG.severe("Failed to set topology state.");
      // Check whether the topology has already been running
      Boolean isTopologyRunning =
          NetworkUtils.awaitResult(statemgr.isTopologyRunning(topology.getName()), 5, TimeUnit.SECONDS);

      if (isTopologyRunning != null && isTopologyRunning.equals(Boolean.TRUE)) {
        LOG.severe("Topology already exists");
      }

      return false;
    }

    return true;
  }

  /**
   * Release the topology lock by trying to remove the trimmed topology definition from the state manager
   * Make sure this method is invoked after doing any operations on killing a topology.
   * People can do the submission again once the lock is released.
   *
   * @param statemgr the state manager to persist the lock
   * @param topologyName the topologyName to remove lock
   * <p/>
   * * @return true if released lock successfully
   */
  public static boolean release(SchedulerStateManagerAdaptor statemgr, String topologyName) {
    ListenableFuture<Boolean> booleanFuture;
    Boolean futureResult;

    booleanFuture = statemgr.deleteTopology(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      LOG.severe("Failed to clear topology state");
      return false;
    }

    return true;
  }
}
