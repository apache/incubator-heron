package com.twitter.heron.scheduler.mesos;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.scheduler.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.scheduler.context.RuntimeManagerContext;

import com.twitter.heron.scheduler.util.NetworkUtility;

public class MesosTopologyRuntimeManager implements IRuntimeManager {
  private static final Logger LOG = Logger.getLogger(MesosTopologyRuntimeManager.class.getName());

  private RuntimeManagerContext context;
  private String topologyName;
  private SchedulerStateManagerAdaptor stateManager;

  @Override
  public void initialize(RuntimeManagerContext context) {
    this.context = context;
    this.topologyName = context.getTopologyName();
  }

  @Override
  public void close() {

  }

  @Override
  public boolean prepareDeactivate() {
    return true;
  }

  @Override
  public boolean postDeactivate() {
    return true;
  }

  @Override
  public boolean prepareActivate() {
    return true;
  }

  @Override
  public boolean postActivate() {
    return true;
  }

  @Override
  /**
   * For kill a mesos topology, we need to:
   * 1. Send a kill request to HSS and wait for the response.
   * 2. If the response is true, we then send kill request to topology scheduler.
   */
  public boolean prepareKill() {
    String jobName = topologyName + "-framework";
    LOG.info("Sending kill request to HSS");

    String endpoint = String.format("%s/%s",
        context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ENDPOINT),
        "kill");
    HttpURLConnection connection;
    try {
      connection = NetworkUtility.getConnection(endpoint);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to endpoint: " + endpoint);

      return false;
    }

    if (!NetworkUtility.sendHttpPostRequest(connection, jobName.getBytes())) {
      LOG.severe("Failed to send http request");
      connection.disconnect();

      return false;
    }

    try {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        LOG.severe("Response code is not ok: " + connection.getResponseCode());

        return false;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get response code", e);
    } finally {
      connection.disconnect();
    }

    LOG.info("Received OK response ok from HSS.");

    return true;
  }

  @Override
  public boolean postKill() {
    // We would not remove clean the package
    return true;
  }

  @Override
  public boolean prepareRestart(int containerIndex) {
    // Nothing to do here. Scheduler will take care of restarting topology shard.
    return true;
  }

  @Override
  public boolean postRestart(int containerIndex) {
    return true;
  }
}
