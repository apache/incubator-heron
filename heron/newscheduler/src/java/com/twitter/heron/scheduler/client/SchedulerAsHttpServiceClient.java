package com.twitter.heron.scheduler.client;

import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.spi.common.HttpUtils;

/**
 * This class manages topology by sending request
 * to appropriate HTTP endpoint of the topology scheduler service.
 */

public class SchedulerAsHttpServiceClient implements ISchedulerClient {
  private static final Logger LOG = Logger.getLogger(SchedulerAsHttpServiceClient.class.getName());

  private final HttpURLConnection connection;

  public SchedulerAsHttpServiceClient(HttpURLConnection connection) {
    this.connection = connection;
  }

  @Override
  public boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest) {
    return requestSchedulerService(restartTopologyRequest.toByteArray());
  }

  @Override
  public boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest) {
    return requestSchedulerService(killTopologyRequest.toByteArray());
  }

  /**
   * Send payload to target HTTP connection to request a service
   *
   * @param data the byte[] to send
   * @return true if got OK response successfully
   */
  protected boolean requestSchedulerService(byte[] data) {
    if (connection == null) {
      LOG.severe("Scheduler not found.");
      return false;
    } else {
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
          LOG.info("Receiving response from scheduler...");
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
    }

    return true;
  }
}
