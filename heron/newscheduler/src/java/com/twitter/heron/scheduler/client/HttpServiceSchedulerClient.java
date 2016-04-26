package com.twitter.heron.scheduler.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.common.Command;

/**
 * This class manages topology by sending request
 * to appropriate HTTP endpoint of the topology scheduler service.
 */

public class HttpServiceSchedulerClient implements ISchedulerClient {
  private static final Logger LOG = Logger.getLogger(HttpServiceSchedulerClient.class.getName());

  private final Config config;
  private final Config runtime;
  private final String schedulerHttpEndpoint;

  public HttpServiceSchedulerClient(Config config, Config runtime,
                                    String schedulerHttpEndpoint) {
    this.config = config;
    this.runtime = runtime;
    this.schedulerHttpEndpoint = schedulerHttpEndpoint;
  }

  @Override
  public boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest) {
    return requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());
  }

  @Override
  public boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest) {
    return requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());
  }

  /**
   * Send payload to target HTTP connection to request a service
   *
   * @param data the byte[] to send
   * @return true if got OK response successfully
   */
  protected boolean requestSchedulerService(Command command, byte[] data) {
    final HttpURLConnection connection = createHttpConnection(command);
    if (connection == null) {
      LOG.severe("Scheduler not found.");
      return false;
    }

    // now, we have a valid connection
    try {
      // send the actual http request
      if (!HttpUtils.sendHttpPostRequest(connection, data)) {
        LOG.log(Level.SEVERE, "Failed to send http request to scheduler");
        return false;
      }

      // receive the response for manage topology
      Common.StatusCode statusCode;
      try {
        LOG.fine("Receiving response from scheduler...");
        statusCode = Scheduler.SchedulerResponse.newBuilder()
            .mergeFrom(HttpUtils.readHttpResponse(connection))
            .build().getStatus().getStatus();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Failed to parse response from scheduler: ", e);
        return false;
      }

      if (!statusCode.equals(Common.StatusCode.OK)) {
        LOG.severe("Received not OK response from scheduler");
        return false;
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to communicate with Scheduler: ", e);
      return false;
    } finally {
      connection.disconnect();
    }

    return true;
  }

  /**
   * Create a http connection, if the scheduler end point is present
   */
  protected HttpURLConnection createHttpConnection(Command command) {
    // construct the http request for command
    String endpoint = getCommandEndpoint(schedulerHttpEndpoint, command);

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
   * Construct the endpoint to send http request for a particular command
   * Make sure the construction matches server sides.
   *
   * @param schedulerEndpoint The scheduler http endpoint
   * @param command The command to request
   * @return The http endpoint for particular command
   */
  protected String getCommandEndpoint(String schedulerEndpoint, Command command) {
    // Currently the server side receives command request in lower case
    return String.format("http://%s/%s", schedulerEndpoint, command.name().toLowerCase());
  }
}
