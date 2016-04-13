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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class TMasterUtils {
  private TMasterUtils() {

  }

  private static final Logger LOG = Logger.getLogger(TMasterUtils.class.getName());

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
