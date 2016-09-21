// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public final class TMasterUtils {
  public enum TMasterCommand {
    ACTIVATE,
    DEACTIVATE
  }

  private static final Logger LOG = Logger.getLogger(TMasterUtils.class.getName());

  private TMasterUtils() {
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
    LOG.fine("Fetching TMaster location for topology: " + topologyName);

    TopologyMaster.TMasterLocation location = stateManager.getTMasterLocation(topologyName);
    if (location == null) {
      LOG.severe("Failed to fetch TMaster Location for topology: " + topologyName);
      return false;
    }
    LOG.fine("Fetched TMaster location for topology: " + topologyName);

    // for the url request to be sent to TMaster
    String endpoint = String.format("http://%s:%d/%s?topologyid=%s",
        location.getHost(), location.getControllerPort(), command, location.getTopologyId());
    LOG.fine("HTTP URL for TMaster: " + endpoint);

    // create a URL connection
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(endpoint).openConnection();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get a HTTP connection to TMaster: ", e);
      return false;
    }
    LOG.fine("Successfully opened HTTP connection to TMaster");

    // now sent the http request
    NetworkUtils.sendHttpGetRequest(connection);
    LOG.fine("Sent the HTTP payload to TMaster");

    boolean success = false;
    // get the response and check if it is successful
    try {
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        LOG.fine("Successfully got a HTTP response from TMaster for " + command);
        success = true;
      } else {
        LOG.fine(String.format("Non OK HTTP response %d from TMaster for command %s",
            responseCode, command));
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE,
          "Failed to receive HTTP response from TMaster for " + command + " :", e);
    } finally {
      connection.disconnect();
    }

    return success;
  }


  /**
   * Get current running TopologyState
   */
  public static TopologyAPI.TopologyState getRuntimeTopologyState(
      String topologyName,
      SchedulerStateManagerAdaptor statemgr) {
    PhysicalPlans.PhysicalPlan plan = statemgr.getPhysicalPlan(topologyName);

    if (plan == null) {
      LOG.log(Level.SEVERE, "Failed to get physical plan for topology {0}", topologyName);
      return null;
    }

    return plan.getTopology().getState();
  }

  public static boolean transitionTopologyState(String topologyName,
                                                TMasterCommand topologyStateControlCommand,
                                                SchedulerStateManagerAdaptor statemgr,
                                                TopologyAPI.TopologyState startState,
                                                TopologyAPI.TopologyState expectedState) {
    TopologyAPI.TopologyState state = TMasterUtils.getRuntimeTopologyState(topologyName, statemgr);
    if (state == null) {
      LOG.severe("Topology still not initialized.");
      return false;
    }

    if (state == expectedState) {
      LOG.log(Level.SEVERE, "Topology {0} command received topology {1} but already in {2} state",
          new Object[] {topologyStateControlCommand, topologyName, state});
      return true;
    }

    if (state != startState) {
      LOG.log(Level.SEVERE, "Topology not in {0} state", startState);
      return false;
    }

    if (!TMasterUtils.sendToTMaster(
        topologyStateControlCommand.name().toLowerCase(), topologyName, statemgr)) {
      LOG.log(Level.SEVERE, "Failed to {0} topology: {1} ",
          new Object[]{topologyStateControlCommand, topologyName});
      return false;
    }

    LOG.log(Level.INFO,
        "Topology command {0} completed successfully.", topologyStateControlCommand);
    return true;
  }
}
