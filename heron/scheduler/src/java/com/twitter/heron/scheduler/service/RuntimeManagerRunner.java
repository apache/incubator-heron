package com.twitter.heron.scheduler.service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;

import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.scheduler.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.scheduler.context.RuntimeManagerContext;

import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager;

// TODO(mfu): Should we break it into different handlers?
public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  private final RuntimeManagerContext context;
  private final String topologyName;
  private final IRuntimeManager.Command command;
  private final IRuntimeManager runtimeManager;

  public RuntimeManagerRunner(IRuntimeManager.Command command,
                              IRuntimeManager runtimeManager,
                              RuntimeManagerContext context) {
    this.command = command;
    this.runtimeManager = runtimeManager;
    this.context = context;
    this.topologyName = this.context.getTopologyName();
  }

  @Override
  public Boolean call() {
    runtimeManager.initialize(context);
    SchedulerStateManagerAdaptor stateManager = context.getStateManagerAdaptor();

    boolean res = prepare();

    if (res) {
      // Create a HTTP request to inform Scheduler
      res = communicateScheduler(stateManager);

      if (res) {
        res = post();
      } else {
        LOG.severe(String.format(" Failed to communicate scheduler to %s remotely", command.name()));
      }
    }

    runtimeManager.close();

    return res;
  }

  protected boolean prepare() {
    boolean ret = false;

    switch (command) {
      case ACTIVATE:
        ret = runtimeManager.prepareActivate();
        break;
      case DEACTIVATE:
        ret = runtimeManager.prepareDeactivate();
        break;
      case RESTART:
        ret = runtimeManager.prepareRestart(context.getRestartShard());
        break;
      case KILL:
        ret = runtimeManager.prepareKill();
        break;
      default:
        throw new IllegalArgumentException("Unknown command to manage topology");
    }

    if (!ret) {
      LOG.severe(String.format("Failed to prepare %s locally", command.name()));
      return false;
    }

    return true;
  }

  protected boolean post() {
    boolean ret = false;

    switch (command) {
      case ACTIVATE:
        ret = runtimeManager.postActivate();
        break;
      case DEACTIVATE:
        ret = runtimeManager.postDeactivate();
        break;
      case RESTART:
        ret = runtimeManager.postRestart(context.getRestartShard());
        break;
      case KILL:
        ret = runtimeManager.postKill() && cleanState(context.getStateManagerAdaptor());
        break;
      default:
        throw new IllegalArgumentException("Unknown command to manage topology");
    }

    if (!ret) {
      LOG.severe(String.format("Failed to post %s locally", command.name()));
      return false;
    }

    return true;
  }

  protected boolean communicateScheduler(SchedulerStateManagerAdaptor stateManager) {
    LOG.info(String.format("Communicating scheduler to %s topology", command.name()));

    // 1. Fetch scheduler location from state manager
    Scheduler.SchedulerLocation schedulerLocation =
        NetworkUtility.awaitResult(stateManager.getSchedulerLocation(),
            5,
            TimeUnit.SECONDS);

    if (schedulerLocation == null) {
      LOG.severe("Failed to get Scheduler Location");

      return false;
    }

    if (schedulerLocation.getHttpEndpoint().equals(CuratorStateManager.NO_SCHEDULER_REST_ENDPOINT)) {
      LOG.info("Nothing required to be done on scheduler.");
      return true;
    }
    LOG.info("Scheduler Location: " + schedulerLocation.toString());

    // 2. Send request to scheduler
    // Construct the HttpUrlConnection
    String endpoint = getCommandEndpoint(schedulerLocation, command);
    HttpURLConnection connection;
    try {
      connection = NetworkUtility.getConnection(endpoint);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to endpoint: " + endpoint);

      return false;
    }
    if (!sendCommandRequest(connection, command)) {
      LOG.log(Level.SEVERE, "Failed to send request: " + endpoint);

      return false;
    }

    // 3. Check whether we activated the topology successfully
    if (!isRequestSuccessful(connection, command)) {
      LOG.severe(String.format("Failed to request %s remotely", command.name()));

      return false;
    }

    // Clean the connection when we are done.
    connection.disconnect();

    LOG.info("Scheduler activated topology successfully.");

    return true;
  }

  protected String getCommandEndpoint(Scheduler.SchedulerLocation location, IRuntimeManager.Command command) {
    return String.format("http://%s/%s",
        location.getHttpEndpoint(),
        command.name().toLowerCase());
  }

  protected boolean sendCommandRequest(HttpURLConnection connection, IRuntimeManager.Command command) {
    LOG.info(String.format("Send %s request to scheduler...", command.name()));
    byte[] data;

    switch (command) {
      case ACTIVATE:
        data = Scheduler.ActivateTopologyRequest.newBuilder().
            setTopologyName(topologyName).
            build().toByteArray();
        break;
      case DEACTIVATE:
        data = Scheduler.DeactivateTopologyRequest.newBuilder().
            setTopologyName(topologyName).
            build().toByteArray();
        break;
      case RESTART:
        data = Scheduler.RestartTopologyRequest.newBuilder().
            setTopologyName(topologyName).
            setContainerIndex(context.getRestartShard()).
            build().toByteArray();
        break;
      case KILL:
        data = Scheduler.KillTopologyRequest.newBuilder().
            setTopologyName(topologyName).
            build().toByteArray();
        break;
      default:
        throw new IllegalArgumentException("Unknown command to manage topology");
    }
    return NetworkUtility.sendHttpPostRequest(connection, data);
  }

  /**
   * Check whether we activated a topology successfully
   *
   * @return true only when we could get the response and the response is ok
   */
  protected boolean isRequestSuccessful(HttpURLConnection connection, IRuntimeManager.Command command) {
    LOG.info(String.format("Received %s response from scheduler...", command.name()));
    Common.StatusCode statusCode;
    try {
      switch (command) {
        case ACTIVATE:
          statusCode = Scheduler.ActivateTopologyResponse.newBuilder().
              mergeFrom(NetworkUtility.readHttpResponse(connection)).build().getStatus().getStatus();
          break;
        case DEACTIVATE:
          statusCode = Scheduler.DeactivateTopologyResponse.newBuilder().
              mergeFrom(NetworkUtility.readHttpResponse(connection)).build().getStatus().getStatus();
          break;
        case RESTART:
          statusCode = Scheduler.RestartTopologyResponse.newBuilder().
              mergeFrom(NetworkUtility.readHttpResponse(connection)).build().getStatus().getStatus();
          break;
        case KILL:
          statusCode = Scheduler.KillTopologyResponse.newBuilder().
              mergeFrom(NetworkUtility.readHttpResponse(connection)).build().getStatus().getStatus();
          break;
        default:
          throw new IllegalArgumentException("Unknown command to manage topology");
      }

    } catch (Exception e) {
      LOG.log(Level.SEVERE, String.format("Failed to parse the %s response: ", command.name()) + e);

      return false;
    }

    return statusCode.equals(Common.StatusCode.OK);
  }

  protected boolean cleanState(SchedulerStateManagerAdaptor stateManager) {
    LOG.info("Cleaning up Heron State");
    try {
      if (!NetworkUtility.awaitResult(stateManager.clearPhysicalPlan(), 5, TimeUnit.SECONDS)) {
        // We would not return false since it is possbile that TMaster didn't write physical plan
        LOG.severe("Failed to clear physical plan. Check whether TMaster set it correctly.");
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to clear physical plan", e);
    }
    
    if (!NetworkUtility.awaitResult(stateManager.clearExecutionState(), 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to clear execution state");
      return false;
    }

    if (!NetworkUtility.awaitResult(stateManager.clearTopology(), 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to clear topology state");
      return false;
    }

    LOG.info("Cleaned up Heron State");
    return true;
  }
}
