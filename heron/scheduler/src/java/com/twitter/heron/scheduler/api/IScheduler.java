package com.twitter.heron.scheduler.api;


import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.api.context.LaunchContext;

/**
 * Scheduler object responsible for bringing up topology. Will be instantiated using no-arg
 * constructor.
 */
public interface IScheduler {
  /**
   * This will initialize scheduler using config file. Will be called during start.
   */
  void initialize(LaunchContext context);

  /**
   * This method will be called after initialize.
   * It is responsible for grabbing resource to launch executor and make sure they
   * get launched.
   * <p/>
   *
   * @param packing Initial mapping suggested by running packing algorithm.
   */
  void schedule(PackingPlan packing);

  /**
   * When health check is received from a host, this method will get called.
   *
   * @param healthCheckResponse Executor response from health-check.
   */
  void onHealthCheck(String healthCheckResponse);

  /**
   * Called by SchedulerServer when it receives a http request to kill topology,
   * while the http request body would be the protobuf Scheduler.KillTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The KillTopologyRequest sent from local heron-cli
   * @return true if the IScheduler kills the topology successfully. SchedulerServer would
   * send KillTopologyResponse correspondingly according to this method's return value.
   */
  boolean onKill(Scheduler.KillTopologyRequest request);

  /**
   * Called by SchedulerServer when it receives a http request to activate topology,
   * while the http request body would be the protobuf Scheduler.ActivateTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The ActivateTopologyRequest sent from local heron-cli
   * @return true if the IScheduler activates the topology successfully. SchedulerServer would
   * send ActivateTopologyResponse correspondingly according to this method's return value.
   */
  boolean onActivate(Scheduler.ActivateTopologyRequest request);

  /**
   * Called by SchedulerServer when it receives a http request to deactivate topology,
   * while the http request body would be the protobuf Scheduler.DeactivateTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The DeactivateTopologyRequest sent from local heron-cli
   * @return true if the IScheduler deactivates the topology successfully. SchedulerServer would
   * send DeactivateTopologyResponse correspondingly according to this method's return value.
   */
  boolean onDeactivate(Scheduler.DeactivateTopologyRequest request);

  /**
   * Called by SchedulerServer when it receives a http request to restart topology,
   * while the http request body would be the protobuf Scheduler.RestartTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The RestartTopologyRequest sent from local heron-cli
   * @return true if the IScheduler restarts the topology successfully. SchedulerServer would
   * send RestartTopologyResponse correspondingly according to this method's return value.
   */
  boolean onRestart(Scheduler.RestartTopologyRequest request);
}