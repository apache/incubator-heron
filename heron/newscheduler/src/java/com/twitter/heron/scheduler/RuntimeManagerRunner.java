package com.twitter.heron.scheduler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyLock;

public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  private final Config config;
  private final Config runtime;
  private final IRuntimeManager.Command command;
  private final IRuntimeManager runtimeManager;

  public RuntimeManagerRunner(Config config, Config runtime,
                              IRuntimeManager.Command command) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    this.config = config;
    this.runtime = runtime;
    this.command = command;
    this.runtimeManager = Runtime.runtimeManagerClassInstance(runtime);
  }

  @Override
  public Boolean call() {

    // initialize the runtime manager
    runtimeManager.initialize(config, runtime);

    // execute the appropriate command
    boolean result = false;
    switch (command) {
      case ACTIVATE:
        result = activateTopologyHandler(Context.topologyName(config));
        break;
      case DEACTIVATE:
        result = deactivateTopologyHandler(Context.topologyName(config));
        break;
      case RESTART:
        result = restartTopologyHandler(Context.topologyName(config));
        break;
      case KILL:
        result = killTopologyHandler(Context.topologyName(config));
        break;
      default:
        LOG.severe("Unknown command for topology: " + command);
    }

    return result;
  }

  /**
   * Create a http connection, if the scheduler end point is present
   */
  protected HttpURLConnection createHttpConnection() {
    // TODO(mfu): Add Proxy support, rather than to connect scheduler directly

    // get the instance of the state manager
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);

    // fetch scheduler location from state manager
    LOG.log(Level.INFO, "Fetching scheduler location from state manager to {0} topology", command);

    ListenableFuture<Scheduler.SchedulerLocation> locationFuture =
        statemgr.getSchedulerLocation(null, Runtime.topologyName(runtime));

    Scheduler.SchedulerLocation schedulerLocation =
        NetworkUtils.awaitResult(locationFuture, 5, TimeUnit.SECONDS);

    if (schedulerLocation == null) {
      LOG.log(Level.INFO, "Failed to get scheduler location to {0} topology", command);
      return null;
    }

    LOG.info("Scheduler is listening on location: " + schedulerLocation.toString());

    // construct the http request for command
    String endpoint = getCommandEndpoint(schedulerLocation.getHttpEndpoint(), command);

    // construct the http url connection
    HttpURLConnection connection;
    try {
      connection = HttpUtils.getConnection(endpoint);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to scheduler http endpoint: {0}", endpoint);
      return null;
    }

    return connection;
  }

  /**
   * Handler to activate a topology
   */
  protected boolean activateTopologyHandler(String topologyName) {
    TopologyAPI.TopologyState state = getRuntimeTopologyState(topologyName);
    if (state == null) {
      LOG.severe("Topology still not initialized.");
      return false;
    }
    if (state == TopologyAPI.TopologyState.RUNNING) {
      LOG.warning("Topology is already activated");
      return true;
    }

    // call prepare to activate
    if (!runtimeManager.prepareActivate()) {
      LOG.severe("Failed to prepare activate locally");
      return false;
    }

    // create the http connection, if scheduler exists
    final HttpURLConnection connection = createHttpConnection();
    if (connection == null) {
      return false;
    }

    // now, we have a valid connection
    try {
      // form the activate topology request payload
      byte[] data = Scheduler.ActivateTopologyRequest.newBuilder()
          .setTopologyName(topologyName).build().toByteArray();

      // send the actual http request
      if (!HttpUtils.sendHttpPostRequest(connection, data)) {
        LOG.log(Level.SEVERE, "Failed to send http request for activate");
        return false;
      }

      // receive the response for activate topology
      Common.StatusCode statusCode;
      try {
        LOG.info("Receiving activate response from scheduler...");
        statusCode = Scheduler.ActivateTopologyResponse.newBuilder()
            .mergeFrom(HttpUtils.readHttpResponse(connection))
            .build().getStatus().getStatus();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Failed to parse activate response: " + e);
        return false;
      }

      if (!statusCode.equals(Common.StatusCode.OK)) {
        LOG.severe("Received not OK response from scheduler for activate");
        return false;
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to activate with Scheduler: ", e);
      return false;
    } finally {
      connection.disconnect();
    }

    // call post activate
    if (!runtimeManager.postActivate()) {
      LOG.severe("Failed in post activate locally");
      return false;
    }

    // Clean the connection when we are done.
    LOG.info("Scheduler activated topology successfully.");
    return true;
  }

  /**
   * Handler to deactivate a topology
   */
  protected boolean deactivateTopologyHandler(String topologyName) {
    TopologyAPI.TopologyState state = getRuntimeTopologyState(topologyName);
    if (state == null) {
      LOG.severe("Topology still not initialized.");
      return false;
    }
    if (state == TopologyAPI.TopologyState.PAUSED) {
      LOG.warning("Topology is already deactivated");
      return true;
    }

    // call prepare to deactivate
    if (!runtimeManager.prepareDeactivate()) {
      LOG.severe("Failed to prepare deactivate locally");
      return false;
    }

    // create the http connection, if scheduler exists
    final HttpURLConnection connection = createHttpConnection();
    if (connection == null) {
      return false;
    }

    // now, we have a valid connection
    try {
      // form the deactivate topology request payload
      byte[] data = Scheduler.DeactivateTopologyRequest.newBuilder()
          .setTopologyName(topologyName).build().toByteArray();

      // send the actual http request
      if (!HttpUtils.sendHttpPostRequest(connection, data)) {
        LOG.log(Level.SEVERE, "Failed to send http request for deactivate");
        return false;
      }

      // receive the response for deactivate topology
      Common.StatusCode statusCode;
      LOG.info("Receiving deactivate response from scheduler...");
      statusCode = Scheduler.DeactivateTopologyResponse.newBuilder()
          .mergeFrom(HttpUtils.readHttpResponse(connection))
          .build().getStatus().getStatus();


      if (!statusCode.equals(Common.StatusCode.OK)) {
        LOG.severe("Received not OK response from scheduler for deactivate");
        return false;
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to deactivate with Scheduler: ", e);
      return false;
    } finally {
      connection.disconnect();
    }

    // call post deactivate
    if (!runtimeManager.postDeactivate()) {
      LOG.severe("Failed in post deactivate locally");
      return false;
    }

    LOG.info("Scheduler deactivated topology successfully.");
    return true;
  }

  /**
   * Handler to restart a topology (TODO - restart a shard)
   */
  protected boolean restartTopologyHandler(String topologyName) {

    // get the container id
    Integer containerId = Context.topologyContainerId(config);

    // call prepare to restart
    if (!runtimeManager.prepareRestart(containerId)) {
      LOG.severe("Failed to prepare restart locally");
      return false;
    }

    // create the http connection, if scheduler exists
    final HttpURLConnection connection = createHttpConnection();
    if (connection == null) {
      return false;
    }

    // now, we have a valid connection
    try {
      // form the restart topology request payload
      byte[] data = Scheduler.RestartTopologyRequest.newBuilder()
          .setTopologyName(topologyName)
          .setContainerIndex(containerId)
          .build().toByteArray();

      // send the actual http request
      if (!HttpUtils.sendHttpPostRequest(connection, data)) {
        LOG.log(Level.SEVERE, "Failed to send http request for restart ");
        return false;
      }

      // receive the response for restart topology
      Common.StatusCode statusCode;
      try {
        LOG.info("Receiving restart response from scheduler...");
        statusCode = Scheduler.RestartTopologyResponse.newBuilder()
            .mergeFrom(HttpUtils.readHttpResponse(connection))
            .build().getStatus().getStatus();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Failed to parse restart response: ", e);
        return false;
      }

      if (!statusCode.equals(Common.StatusCode.OK)) {
        LOG.severe("Received not OK response from scheduler for restart");
        return false;
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to restart with Scheduler: ", e);
      return false;
    } finally {
      connection.disconnect();
    }

    // call post restart
    if (!runtimeManager.postRestart(containerId)) {
      LOG.severe("Failed in post restart locally");
      return false;
    }

    // Clean the connection when we are done.
    LOG.info("Scheduler restarted topology successfully.");
    return true;
  }

  /**
   * Handler to kill a topology
   */
  protected boolean killTopologyHandler(String topologyName) {

    // call prepare to kill
    if (!runtimeManager.prepareKill()) {
      LOG.severe("Failed to prepare kill locally");
      return false;
    }

    // create the http connection, if scheduler exists
    final HttpURLConnection connection = createHttpConnection();
    if (connection == null) {
      // It is possible that the scheduler has not yet started.
      LOG.info("Scheduler Not exists.");
    } else {
      // now, we have a valid connection
      try {
        // form the kill topology request payload
        byte[] data = Scheduler.KillTopologyRequest.newBuilder()
            .setTopologyName(topologyName).build().toByteArray();

        // send the actual http request
        if (!HttpUtils.sendHttpPostRequest(connection, data)) {
          LOG.log(Level.SEVERE, "Failed to send http request for kill");
          return false;
        }

        // receive the response for kill topology
        Common.StatusCode statusCode;
        try {
          LOG.info("Receiving kill response from scheduler...");
          statusCode = Scheduler.KillTopologyResponse.newBuilder()
              .mergeFrom(HttpUtils.readHttpResponse(connection))
              .build().getStatus().getStatus();
        } catch (Exception e) {
          LOG.log(Level.SEVERE, "Failed to parse kill response: ", e);
          return false;
        }

        if (!statusCode.equals(Common.StatusCode.OK)) {
          LOG.severe("Received not OK response from scheduler for kill");
          return false;
        }
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Failed to kill with Scheduler: ", e);
        return false;
      } finally {
        connection.disconnect();
      }
    }

    // call post kill
    if (!runtimeManager.postKill()) {
      LOG.severe("Failed in post deactivate locally");
      return false;
    }

    // clean up the state of the topology in state manager
    if (!cleanState(topologyName)) {
      LOG.severe("Failed in clean state");
      return false;
    }

    // Clean the connection when we are done.
    LOG.info("Scheduler killed topology successfully.");
    return true;
  }

  /**
   * Clean the various state of heron topology
   */
  protected boolean cleanState(String topologyName) {
    LOG.info("Cleaning up Heron State");

    // get the instance of the state manager
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);

    ListenableFuture<Boolean> booleanFuture;
    Boolean futureResult;

    // It is possible that  TMasterLocation, PhysicalPlan and SchedulerLocation are not set
    // Just log but don't consider them failure
    booleanFuture = statemgr.deleteTMasterLocation(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.severe("Failed to clear TMaster location. Check whether TMaster set it correctly.");
    }

    booleanFuture = statemgr.deletePhysicalPlan(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.severe("Failed to clear physical plan. Check whether TMaster set it correctly.");
    }

    booleanFuture = statemgr.deleteSchedulerLocation(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      // We would not return false since it is possible that TMaster didn't write physical plan
      LOG.severe("Failed to clear scheduler location. Check whether Scheduler set it correctly.");
    }

    booleanFuture = statemgr.deleteExecutionState(topologyName);
    futureResult = NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS);
    if (futureResult == null || !futureResult) {
      // We would not return false since it is possible that
      // people terminate submission before writing execution state
      LOG.severe("Failed to clear execution state");
    }

    // Finally, release the topology lock
    if (!TopologyLock.release(statemgr, topologyName)) {
      LOG.severe("Failed to release the topology lock");
    }

    LOG.info("Cleaned up Heron State");
    return true;
  }

  /**
   * Get current running TopologyState
   */
  protected TopologyAPI.TopologyState getRuntimeTopologyState(String topologyName) {
    // get the instance of the state manager
    SchedulerStateManagerAdaptor statemgr = Runtime.schedulerStateManagerAdaptor(runtime);
    ListenableFuture<PhysicalPlans.PhysicalPlan> physicalPlanFuture = statemgr.getPhysicalPlan(null, topologyName);
    PhysicalPlans.PhysicalPlan plan =
        NetworkUtils.awaitResult(physicalPlanFuture, 5, TimeUnit.SECONDS);

    if (plan == null) {
      LOG.log(Level.SEVERE, "Failed to get physical plan for topology {0}", topologyName);
      return null;
    }

    return plan.getTopology().getState();
  }

  /**
   * Construct the endpoint to send http request for a particular command
   * Make sure the construction matches server sides.
   *
   * @param schedulerEndpoint The scheduler http endpoint
   * @param command The command to request
   * @return The http endpoint for particular command
   */
  protected String getCommandEndpoint(String schedulerEndpoint, IRuntimeManager.Command command) {
    // Currently the server side receives command request in lower case
    return String.format("http://%s/%s", schedulerEndpoint, command.name().toLowerCase());
  }
}
