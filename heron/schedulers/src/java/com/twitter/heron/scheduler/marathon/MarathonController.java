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

package com.twitter.heron.scheduler.marathon;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.utils.NetworkUtils;

import sun.nio.ch.Net;

public class MarathonController {
  private static final Logger LOG = Logger.getLogger(MarathonController.class.getName());

  private final String marathonURI;
  private final String topologyName;
  private final boolean isVerbose;

  public MarathonController(
      String marathonURI,
      String topologyName,
      boolean isVerbose
  ) {
    this.marathonURI = marathonURI;
    this.topologyName = topologyName;
    this.isVerbose = isVerbose;
  }

  public boolean killTopology() {
    // Setup Connection
    String topologyURI = String.format("%s/v2/groups/%s", this.marathonURI, this.topologyName);
    HttpURLConnection conn = NetworkUtils.getHttpConnection(topologyURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find marathon scheduler");
      return false;
    }

    // Send kill topology request
    NetworkUtils.sendHttpDeleteRequest(conn);

    // Check response
    boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_OK);
    // Disconnect to release resources
    conn.disconnect();

    if (success) {
      LOG.log(Level.INFO, "Successfully killed topology");
      return true;
    } else {
      LOG.log(Level.SEVERE, "Failed to kill topology");
      return false;
    }
  }

  public boolean restartApp(int appId) {
    if (appId == -1) {
      // TODO (nlu): implement restart all
      throw new RuntimeException("Restart all containers not supported yet");
    }

    // Setup Connection
    String restartRequest = String.format("%s/v2/apps/%s/%d/restart",
        this.marathonURI, this.topologyName, appId);
    HttpURLConnection conn = NetworkUtils.getHttpConnection(restartRequest);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find marathon scheduler");
      return false;
    }

    // send post request to restart app
    byte[] empty = new byte[0];
    if (!NetworkUtils.sendHttpPostRequest(conn, NetworkUtils.JSON_TYPE, empty)) {
      LOG.log(Level.SEVERE, "Failed to set post request");
      conn.disconnect();
      return false;
    }

    // Check response
    boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_OK);
    // Disconnect to release resources
    conn.disconnect();

    if (success) {
      LOG.log(Level.INFO, "Successfully restarted container {0}", appId);
      return true;
    } else {
      LOG.log(Level.SEVERE, "Failed to restart container {0}", appId);
      return false;
    }
  }

  // submit a topology as a group, containers as apps in the group
  public boolean submitTopology(String appConf) {
    if (this.isVerbose) {
      LOG.log(Level.INFO, "Topology conf is: " + appConf);
    }

    // Setup Connection
    String schedulerURI = String.format("%s/v2/groups", this.marathonURI);
    HttpURLConnection conn = NetworkUtils.getHttpConnection(schedulerURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find marathon scheduler");
      return false;
    }

    // Send post request with marathon conf for topology
    if (!NetworkUtils.sendHttpPostRequest(conn, NetworkUtils.JSON_TYPE, appConf.getBytes())) {
      LOG.log(Level.SEVERE, "Failed to ");
      conn.disconnect();
      return false;
    }

    // Check response
    boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_CREATED);
    // Disconnect to release resources
    conn.disconnect();

    if (success) {
      LOG.log(Level.INFO, "Topology submitted successfully");
      return true;
    } else {
      LOG.log(Level.SEVERE, "Failed to submit topology");
      return false;
    }
  }
}
