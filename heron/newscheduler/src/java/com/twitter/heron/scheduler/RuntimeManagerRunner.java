package com.twitter.heron.scheduler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.Pair;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.NetworkUtils;

import com.google.common.util.concurrent.ListenableFuture;

public class RuntimeManagerRunner implements Callable<Boolean> {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerRunner.class.getName());

  private final Config config;
  private final Config runtime;
  private final String command;
  private final IRuntimeManager runtimeManager;

  public RuntimeManagerRunner(Config config, Config runtime, String command) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    this.config = config;
    this.runtime = runtime;
    this.command = command.toLowerCase();

    // create an instance of runtime manager
    String runtimeManagerClass = Context.runtimeManagerClass(config);
    this.runtimeManager = (IRuntimeManager)Class.forName(runtimeManagerClass).newInstance();
  }

  @Override
  public Boolean call() {

    // initialize the runtime manager
    runtimeManager.initialize(config);

    // execute the appropriate command
    boolean result = false;
    if (command.equals("activate"))
      result = activateTopologyHandler(Context.topologyName(config));
    else if (command.equals("deactivate"))
      result = deactivateTopologyHandler(Context.topologyName(config));
    else if (command.equals("restart"))
      result = restartTopologyHandler(Context.topologyName(config));
    else if (command.equals("kill"))
      result = killTopologyHandler(Context.topologyName(config));
    else 
      LOG.info("Unknown command for topology: " + command);

    runtimeManager.close();
    return result;
  }

  /**
   * Create a http connection, if the scheduler end point is present
   */
  protected Pair<Boolean, HttpURLConnection> createHttpConnection() {

    // get the instance of the state manager
    IStateManager statemgr = Runtime.stateManager(runtime);

    // fetch scheduler location from state manager
    LOG.info("Fetching scheduler location from state manager for " + command + " topology");
    
    ListenableFuture<Scheduler.SchedulerLocation> locationFuture = 
        statemgr.getSchedulerLocation(null, Runtime.topologyName(runtime));

    Scheduler.SchedulerLocation schedulerLocation =
        NetworkUtils.awaitResult(locationFuture, 5, TimeUnit.SECONDS);

    if (schedulerLocation == null) {
      LOG.severe("Failed to get scheduler location for " + command + " topology");
      return Pair.create(false, null);
    }

    // if there is no scheduler end point, (e.g) aurora, nothing to do (TODO - eliminate Curator)
    if (schedulerLocation.getHttpEndpoint().equals(statemgr.NO_SCHEDULER_REST_ENDPOINT)) {
      LOG.info("Nothing required to be done on scheduler.");
      return Pair.create(true, null);
    }

    LOG.info("Scheduler is listening on location: " + schedulerLocation.toString());

    // construct the http request for command
    String endpoint = String.format("http://%s/%s", schedulerLocation.getHttpEndpoint(), command);

    // construct the http url connection
    HttpURLConnection connection;
    try {
      connection = HttpUtils.getConnection(endpoint);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to scheduler http endpoint: " + endpoint);
      return Pair.create(false, null);
    }

    return Pair.create(true, connection);
  }

  /**
   * Handler to activate a topology
   */
  protected boolean activateTopologyHandler(String topologyName) {

    // call prepare to activate
    if (!runtimeManager.prepareActivate()) {
      LOG.severe("Failed to prepare activate locally");
      return false;
    }

    // create the http connection, if scheduler exists
    Pair<Boolean, HttpURLConnection> ret = createHttpConnection();
    if (ret.second == null)
      return ret.first;
    
    // now, we have a valid connection
    HttpURLConnection connection = ret.second;

    // form the activate topology request payload
    byte[] data = Scheduler.ActivateTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build().toByteArray();

    // send the actual http request
    if (!HttpUtils.sendHttpPostRequest(connection, data)) {
      LOG.log(Level.SEVERE, "Failed to send http request for activate");
      connection.disconnect();
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
      connection.disconnect();
      return false;
    }

    if (!statusCode.equals(Common.StatusCode.OK)) {
      LOG.severe("Received not OK response from scheduler for activate");
      connection.disconnect();
      return false;
    }

    // call post activate
    if (!runtimeManager.postActivate()) {
      LOG.severe("Failed in post activate locally");
      connection.disconnect();
      return false;
    }

    // Clean the connection when we are done.
    connection.disconnect();
    LOG.info("Scheduler activated topology successfully.");
    return true;
  }

  /**
   * Handler to deactivate a topology
   */
  protected boolean deactivateTopologyHandler(String topologyName) {

    // call prepare to deactivate
    if (!runtimeManager.prepareDeactivate()) {
      LOG.severe("Failed to prepare deactivate locally");
      return false;
    }

    // create the http connection, if scheduler exists
    Pair<Boolean, HttpURLConnection> ret = createHttpConnection();
    if (ret.second == null)
      return ret.first;
    
    // now, we have a valid connection
    HttpURLConnection connection = ret.second;

    // form the deactivate topology request payload
    byte[] data = Scheduler.DeactivateTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build().toByteArray();

    // send the actual http request
    if (!HttpUtils.sendHttpPostRequest(connection, data)) {
      LOG.log(Level.SEVERE, "Failed to send http request for deactivate");
      connection.disconnect();
      return false;
    }

    // receive the response for deactivate topology
    Common.StatusCode statusCode;
    try {
      LOG.info("Receiving deactivate response from scheduler...");
      statusCode = Scheduler.DeactivateTopologyResponse.newBuilder()
          .mergeFrom(HttpUtils.readHttpResponse(connection))
          .build().getStatus().getStatus();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to parse deactivate response: " + e);
      connection.disconnect();
      return false;
    }

    if (!statusCode.equals(Common.StatusCode.OK)) {
      LOG.severe("Received not OK response from scheduler for deactivate");
      connection.disconnect();
      return false;
    }

    // call post deactivate
    if (!runtimeManager.postDeactivate()) {
      LOG.severe("Failed in post deactivate locally");
      connection.disconnect();
      return false;
    }

    // Clean the connection when we are done.
    connection.disconnect();
    LOG.info("Scheduler deactivated topology successfully.");
    return true;
  }

  /**
   * Handler to restart a topology (TODO - restart a shard)
   */
  protected boolean restartTopologyHandler(String topologyName) {

    // call prepare to restart
    if (!runtimeManager.prepareRestart()) {
      LOG.severe("Failed to prepare restart locally");
      return false;
    }

    // create the http connection, if scheduler exists
    Pair<Boolean, HttpURLConnection> ret = createHttpConnection();
    if (ret.second == null)
      return ret.first;
    
    // now, we have a valid connection
    HttpURLConnection connection = ret.second;

    // form the restart topology request payload
    byte[] data = Scheduler.RestartTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build().toByteArray();

    // send the actual http request
    if (!HttpUtils.sendHttpPostRequest(connection, data)) {
      LOG.log(Level.SEVERE, "Failed to send http request for restart ");
      connection.disconnect();
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
      LOG.log(Level.SEVERE, "Failed to parse restart response: " + e);
      connection.disconnect();
      return false;
    }

    if (!statusCode.equals(Common.StatusCode.OK)) {
      LOG.severe("Received not OK response from scheduler for restart");
      connection.disconnect();
      return false;
    }

    // call post restart
    if (!runtimeManager.postRestart()) {
      LOG.severe("Failed in post restart locally");
      connection.disconnect();
      return false;
    }

    // Clean the connection when we are done.
    connection.disconnect();
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
    Pair<Boolean, HttpURLConnection> ret = createHttpConnection();
    if (ret.second == null)
      return ret.first;
    
    // now, we have a valid connection
    HttpURLConnection connection = ret.second;

    // form the kill topology request payload
    byte[] data = Scheduler.KillTopologyRequest.newBuilder()
        .setTopologyName(topologyName).build().toByteArray();

    // send the actual http request
    if (!HttpUtils.sendHttpPostRequest(connection, data)) {
      LOG.log(Level.SEVERE, "Failed to send http request for kill");
      connection.disconnect();
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
      LOG.log(Level.SEVERE, "Failed to parse kill response: " + e);
      connection.disconnect();
      return false;
    }

    if (!statusCode.equals(Common.StatusCode.OK)) {
      LOG.severe("Received not OK response from scheduler for kill");
      connection.disconnect();
      return false;
    }

    // call post kill
    if (!runtimeManager.postKill()) {
      LOG.severe("Failed in post deactivate locally");
      connection.disconnect();
      return false;
    }

    // clean up the state of the topology in state manager
    if (!cleanState(topologyName)) {
      LOG.severe("Failed in clean state");
      connection.disconnect();
      return false;
    }

    // Clean the connection when we are done.
    connection.disconnect();
    LOG.info("Scheduler killed topology successfully.");
    return true;
  }

  /**
   * Clean the various state of heron topology
   */
  protected boolean cleanState(String topologyName) {

    LOG.info("Cleaning up Heron State");

    // get the instance of the state manager
    IStateManager statemgr = Runtime.stateManager(runtime);

    ListenableFuture<Boolean> booleanFuture;
    try {
      booleanFuture = statemgr.deletePhysicalPlan(topologyName);
      if (!NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS)) {
        // We would not return false since it is possbile that TMaster didn't write physical plan
        LOG.severe("Failed to clear physical plan. Check whether TMaster set it correctly.");
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to clear physical plan", e);
    }
    
    booleanFuture = statemgr.deleteExecutionState(topologyName);
    if (!NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to clear execution state");
      return false;
    }

    booleanFuture = statemgr.deleteTopology(topologyName);
    if (!NetworkUtils.awaitResult(booleanFuture, 5, TimeUnit.SECONDS)) {
      LOG.severe("Failed to clear topology state");
      return false;
    }

    LOG.info("Cleaned up Heron State");
    return true;
  }
}
