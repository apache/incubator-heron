package com.twitter.heron.spi.packing;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;

/**
 * Packing algorithm to use for packing multiple instances into containers. Packing hints like
 * number of container may be passed through scheduler config.
 */
public interface IPacking {
  
  /**
   * Initialize the packing algorithm with the static and runtime config 
   */
  public void initialize(Config config, Config runtime);

  /**
   * Called by scheduler to generate container packing.
   * Packing algorithm output generates instance id and container id.
   *
   * @return PackingPlan describing the job to schedule.
   */
  PackingPlan pack();

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   */
  public void close();
}
