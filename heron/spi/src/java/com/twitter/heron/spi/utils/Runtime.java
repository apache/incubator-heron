package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class Runtime {
  private static final Logger LOG = Logger.getLogger(Runtime.class.getName());

  public static String topologyId(Config runtime) {
    return runtime.getStringValue(Keys.topologyId());
  }

  public static String topologyName(Config runtime) {
    return runtime.getStringValue(Keys.topologyName());
  }

  public static String topologyClassPath(Config runtime) {
    return runtime.getStringValue(Keys.topologyClassPath());
  }

  public static TopologyAPI.Topology topology(Config runtime) {
    return (TopologyAPI.Topology) runtime.get(Keys.topologyDefinition());
  }

  public static String topologyPackageUri(Config cfg) {
    return cfg.getStringValue(Keys.topologyPackageUri());
  }

  public static SchedulerStateManagerAdaptor schedulerStateManagerAdaptor(Config runtime) {
    return (SchedulerStateManagerAdaptor) runtime.get(Keys.schedulerStateManagerAdaptor());
  }

  public static IPacking packingClassInstance(Config runtime) {
    return (IPacking) runtime.get(Keys.packingClassInstance());
  }

  public static ILauncher launcherClassInstance(Config runtime) {
    return (ILauncher) runtime.get(Keys.launcherClassInstance());
  }

  public static IRuntimeManager runtimeManagerClassInstance(Config runtime) {
    return (IRuntimeManager) runtime.get(Keys.runtimeManagerClassInstance());
  }

  public static Shutdown schedulerShutdown(Config runtime) {
    return (Shutdown) runtime.get(Keys.schedulerShutdown());
  }

  public static String componentRamMap(Config runtime) {
    return runtime.getStringValue(Keys.componentRamMap());
  }

  public static String componentJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.componentJvmOpts());
  }

  public static String instanceDistribution(Config runtime) {
    return runtime.getStringValue(Keys.instanceDistribution());
  }

  public static String instanceJvmOpts(Config runtime) {
    return runtime.getStringValue(Keys.instanceJvmOpts());
  }

  public static Long numContainers(Config runtime) {
    return runtime.getLongValue(Keys.numContainers());
  }

  /**
   * Communicate with TMaster with command
   *
   * @param command the command requested to TMaster, activate or deactivate.
   * @return true if the requested command is processed successfully by tmaster
   */
  public static boolean sendToTMaster(String command, String topologyName,
                                      SchedulerStateManagerAdaptor stateManager) {
    // fetch the TMasterLocation for the topology
    LOG.info("Fetching TMaster location for topology: " + topologyName);
    ListenableFuture<TopologyMaster.TMasterLocation> locationFuture =
        stateManager.getTMasterLocation(null, topologyName);

    TopologyMaster.TMasterLocation location =
        NetworkUtils.awaitResult(locationFuture, 5, TimeUnit.SECONDS);
    LOG.info("Fetched TMaster location for topology: " + topologyName);

    // for the url request to be sent to TMaster
    String endpoint = String.format("http://%s:%d/%s?topologyid=%s",
        location.getHost(), location.getControllerPort(), command, location.getTopologyId());
    LOG.info("HTTP URL for TMaster: " + endpoint);

    // create a URL connection
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(endpoint).openConnection();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get a HTTP connection to TMaster: ", e);
      return false;
    }
    LOG.info("Successfully opened HTTP connection to TMaster");

    // now sent the http request
    HttpUtils.sendHttpGetRequest(connection);
    LOG.info("Sent the HTTP payload to TMaster");

    boolean success = false;
    // get the response and check if it is successful
    try {
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        LOG.info("Successfully got a HTTP response from TMaster for " + command);
        success = true;
      } else {
        LOG.info(String.format("Non OK HTTP response %d from TMaster for command %s",
            responseCode, command));
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to receive HTTP response from TMaster for " + command + " :", e);
    } finally {
      connection.disconnect();
    }

    return success;
  }
}
