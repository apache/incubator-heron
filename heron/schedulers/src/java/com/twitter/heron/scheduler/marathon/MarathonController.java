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

  protected HttpURLConnection createHttpConnection(String uri) {
    HttpURLConnection connection;
    try {
      connection = NetworkUtils.getConnection(uri);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to create connection for " + uri);
    }

    return connection;
  }

  public boolean killTopology() {
    // Setup Connection
    String topologyURI = String.format("%s/v2/groups/%s", this.marathonURI, this.topologyName);
    HttpURLConnection conn = createHttpConnection(topologyURI);

    try {
      conn.setRequestMethod("DELETE");
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to set delete request: ", e);
      conn.disconnect();
      return false;
    }

    try {
      conn.connect();
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to connect: ", ex);
      return false;
    } finally {
      conn.disconnect();
    }

    // Check response
    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("Failed to kill topology");
      } else {
        LOG.log(Level.INFO, "Topology killed successfully");
      }
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to get response code");
      return false;
    } finally {
      conn.disconnect();
    }

    return true;
  }

  public boolean restartApp(int appId) {
    if (appId == -1) {
      // TODO (nlu): implement restart all
      throw new RuntimeException("Restart all containers not supported yet");
    }

    // Setup Connection
    String restartRequest = String.format("%s/v2/apps/%s/%d/restart",
        this.marathonURI, this.topologyName, appId);
    HttpURLConnection conn = createHttpConnection(restartRequest);

    try {
      conn.setRequestMethod("POST");
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to set post request: ", e);
      conn.disconnect();
      return false;
    }

    try {
      conn.connect();
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to connect: ", ex);
      return false;
    } finally {
      conn.disconnect();
    }

    // Check response
    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        String msg = String.format("Failed to restart container %d" + appId);
        throw new RuntimeException(msg);
      } else {
        String msg = String.format("Container %s restarted successfully", appId);
        LOG.log(Level.INFO, msg);
      }
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to get response code");
      return false;
    } finally {
      conn.disconnect();
    }

    return true;
  }

  // submit a topology as a group, containers as apps in the group
  public boolean submitTopology(String appConf) {
    if (this.isVerbose) {
      LOG.log(Level.INFO, "Topology conf is: " + appConf);
    }

    // Setup Connection
    String schedulerURI = String.format("%s/v2/groups", this.marathonURI);
    HttpURLConnection conn = createHttpConnection(schedulerURI);

    try {
      conn.setRequestMethod("POST");
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to set post request: ", e);
      conn.disconnect();
      return false;
    }
    conn.setDoOutput(true);
    conn.setRequestProperty(NetworkUtils.CONTENT_TYPE, "application/json");

    OutputStream os = null;
    try {
      // Send request
      os = conn.getOutputStream();
      os.write(appConf.getBytes());
      os.flush();
    } catch (IOException exception) {
      LOG.log(Level.SEVERE, "Failed to send the topology conf", exception);
      conn.disconnect();
      return false;
    } finally {
      try {
        if (os != null) {
          os.close();
        }
      } catch (IOException ex) {
        // Do nothing
      }
    }

    // Check response
    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_CREATED) {
        throw new RuntimeException("Failed to submit topology");
      } else {
        LOG.log(Level.INFO, "Topology submitted successfully");
        return true;
      }
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to get response code");
      return false;
    } finally {
      conn.disconnect();
    }
  }
}
