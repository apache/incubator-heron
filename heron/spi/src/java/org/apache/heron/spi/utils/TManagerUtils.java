/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.spi.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class TManagerUtils {
  public enum TManagerCommand {
    ACTIVATE,
    DEACTIVATE,
    RUNTIME_CONFIG_UPDATE
  }

  private static final Logger LOG = Logger.getLogger(TManagerUtils.class.getName());

  private TManagerUtils() {
  }

  /**
   * Communicate with TManager with command
   *
   * @param command the command requested to TManager, activate or deactivate.
   */
  @VisibleForTesting
  public static void sendToTManager(String command,
                                   String topologyName,
                                   SchedulerStateManagerAdaptor stateManager,
                                   NetworkUtils.TunnelConfig tunnelConfig)
      throws TManagerException {
    final List<String> empty = new ArrayList<String>();
    sendToTManagerWithArguments(command, topologyName, empty, stateManager, tunnelConfig);
  }

  @VisibleForTesting
  public static void sendToTManagerWithArguments(String command,
                                                String topologyName,
                                                List<String> arguments,
                                                SchedulerStateManagerAdaptor stateManager,
                                                NetworkUtils.TunnelConfig tunnelConfig)
      throws TManagerException {
    // fetch the TManagerLocation for the topology
    LOG.fine("Fetching TManager location for topology: " + topologyName);

    TopologyManager.TManagerLocation location = stateManager.getTManagerLocation(topologyName);
    if (location == null) {
      throw new TManagerException("Failed to fetch TManager location for topology: "
        + topologyName);
    }

    LOG.fine("Fetched TManager location for topology: " + topologyName);

    // for the url request to be sent to TManager
    String url = String.format("http://%s:%d/%s?topologyid=%s",
        location.getHost(), location.getControllerPort(), command, location.getTopologyId());
    // Append extra url arguments
    for (String arg: arguments) {
      url += "&";
      url += arg;
    }

    try {
      URL endpoint = new URL(url);
      LOG.fine("HTTP URL for TManager: " + endpoint);
      sendGetRequest(endpoint, command, tunnelConfig);
    } catch (MalformedURLException e) {
      throw new TManagerException("Invalid URL for TManager endpoint: " + url, e);
    }
  }

  private static void sendGetRequest(URL endpoint, String command,
                                     NetworkUtils.TunnelConfig tunnelConfig)
      throws TManagerException {
    // create a URL connection
    HttpURLConnection connection =
        NetworkUtils.getProxiedHttpConnectionIfNeeded(endpoint, tunnelConfig);
    if (connection == null) {
      throw new TManagerException(String.format(
          "Failed to get a HTTP connection to TManager: %s", endpoint));
    }
    LOG.fine("Successfully opened HTTP connection to TManager");
    // now sent the http request
    NetworkUtils.sendHttpGetRequest(connection);
    LOG.fine("Sent the HTTP payload to TManager");

    // get the response and check if it is successful
    try {
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        LOG.fine("Successfully got a HTTP response from TManager using command: " + command);
      } else {
        throw new TManagerException(
            String.format("Non OK HTTP response %d from TManager for command %s",
            responseCode, command));
      }
    } catch (IOException e) {
      throw new TManagerException(String.format(
          "Failed to receive HTTP response from TManager using command: `%s`", command), e);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Get current running TopologyState
   */
  private static TopologyAPI.TopologyState getRuntimeTopologyState(
      String topologyName,
      SchedulerStateManagerAdaptor statemgr) throws TManagerException {
    PhysicalPlans.PhysicalPlan plan = statemgr.getPhysicalPlan(topologyName);

    if (plan == null) {
      throw new TManagerException(String.format(
        "Failed to get physical plan for topology '%s'", topologyName));
    }

    return plan.getTopology().getState();
  }

  public static void transitionTopologyState(String topologyName,
                                             TManagerCommand topologyStateControlCommand,
                                             SchedulerStateManagerAdaptor statemgr,
                                             TopologyAPI.TopologyState startState,
                                             TopologyAPI.TopologyState expectedState,
                                             NetworkUtils.TunnelConfig tunnelConfig)
      throws TManagerException {
    TopologyAPI.TopologyState state = TManagerUtils.getRuntimeTopologyState(topologyName, statemgr);
    if (state == null) {
      throw new TManagerException(String.format(
          "Topology '%s' is not initialized yet", topologyName));
    }

    if (state == expectedState) {
      LOG.warning(String.format(
          "Topology %s command received but topology '%s' already in state %s",
          topologyStateControlCommand, topologyName, state));
      return;
    }

    if (state != startState) {
      throw new TManagerException(String.format(
          "Topology '%s' is not in state '%s'", topologyName, startState));
    }

    String command = topologyStateControlCommand.name().toLowerCase();
    TManagerUtils.sendToTManager(command, topologyName, statemgr, tunnelConfig);

    LOG.log(Level.INFO,
        "Topology command {0} completed successfully.", topologyStateControlCommand);
  }

  public static void sendRuntimeConfig(String topologyName,
                                       TManagerCommand topologyStateControlCommand,
                                       SchedulerStateManagerAdaptor statemgr,
                                       String[] configs,
                                       NetworkUtils.TunnelConfig tunnelConfig)
      throws TManagerException {
    final String runtimeConfigKey = "runtime-config";
    final String runtimeConfigUpdateEndpoint = "runtime_config/update";

    List<String> arguments = new ArrayList<String>();
    for (String config: configs) {
      arguments.add(runtimeConfigKey + "=" + config);
    }

    TManagerUtils.sendToTManagerWithArguments(
        runtimeConfigUpdateEndpoint, topologyName, arguments, statemgr, tunnelConfig);

    LOG.log(Level.INFO,
        "Topology command {0} completed successfully.", topologyStateControlCommand);
  }
}
