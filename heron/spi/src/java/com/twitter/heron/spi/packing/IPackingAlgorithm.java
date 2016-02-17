package com.twitter.heron.spi.packing;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

/**
 * Packing algorithm to use for packing multiple instances into containers. Packing hints like
 * number of container may be passed through scheduler config.
 */
public interface IPackingAlgorithm {
  /**
   * Called by scheduler to generate container packing.
   * Packing algorithm output generates instance id and container id.
   *
   * @return PackingPlan describing the job to schedule.
   */
  PackingPlan pack(LaunchContext context);
}
