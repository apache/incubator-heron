package com.twitter.heron.scheduler.api;

import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.api.context.LaunchContext;

/**
 * Launches scheduler. heron-cli will create Launcher object using default no argument constructor.
 */
public interface ILauncher {
  /**
   * Initialize Launcher with Config, Uploader and topology. These object
   * will be passed from submitter main. Config will contain information that launcher may use
   * to setup scheduler and other parameters required by launcher to contact
   * services which will launch scheduler.
   */
  void initialize(LaunchContext context);

  /**
   * Will be called locally before trying to launch topology remotely
   *
   * @return true if successful
   */
  boolean prepareLaunch(PackingPlan packing);

  /**
   * Starts scheduler. Once this function returns successfully, heron-cli will terminate and
   * launch process succeed.
   *
   * @param packing Initial mapping suggested by running packing algorithm.
   * container_id->List of instance_id to be launched on this container.
   * @return true if topology launched successfully, false otherwise.
   */
  boolean launchTopology(PackingPlan packing);

  /**
   * Will be called locally after launching topology remotely
   *
   * @return true if successful
   */
  boolean postLaunch(PackingPlan packing);

  /**
   * In case launch fails, this is called to clean up states.
   */
  void undo();

  /**
   * Add/Modify additional information in execution state. Returns new ExecutionState created using
   * current execution state and adding additional Launch specific information
   * TODO(nbhagat): Don't overload heron's ExecutionState with scheduler specific data.
   *
   * @param executionState Default execution state with all required fields set.
   * @return Updated execution state.
   */
  ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState);
}