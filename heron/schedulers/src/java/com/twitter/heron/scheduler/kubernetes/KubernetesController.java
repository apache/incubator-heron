// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.kubernetes;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.utils.NetworkUtils;

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private final String kubernetesURI;
  private final String topologyName;
  private final boolean isVerbose;

  public KubernetesController(
      String kubernetesURI,
      String topologyName,
      boolean isVerbose
  ) {
    this.kubernetesURI = kubernetesURI;
    this.topologyName = topologyName;
    this.isVerbose = isVerbose;
  }

  /**
   * Kill a topology in kubernetes based on a configuration
   *
   * @return success
   */
  public boolean killTopology() {

    // Setup connection
    String deploymentURI = String.format(
        "%s/api/v1/namespaces/default/pods?labelSelector=topology%%3D%s",
        this.kubernetesURI,
        this.topologyName);

    LOG.log(Level.INFO, deploymentURI);
    HttpURLConnection conn = NetworkUtils.getHttpConnection(deploymentURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find k8s deployment API");
      return false;
    }

    try {
      if (!NetworkUtils.sendHttpDeleteRequest(conn)) {
        LOG.log(Level.SEVERE, "Failed to send delete request to k8s deployment API");
        return false;
      }

      // check response
      boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_OK);

      if (success) {
        LOG.log(Level.SEVERE, "Successfully killed topology deployments");
        return true;
      } else {
        LOG.log(Level.SEVERE, "Failure to delete topology deployments");
        return false;
      }

    } finally {
      // Disconnect to release resources
      conn.disconnect();
    }


  }

  public boolean restartApp(int appId) {
    String message = "Restarting the whole topology is not supported yet. "
        + "Please kill and resubmit the topology.";
    LOG.log(Level.SEVERE, message);
    return false;
  }

  /**
   * Submit a topology to kubernetes based on a configuration
   *
   * @return success
   */
  public boolean submitTopology(String[] appConfs) {

    if (!this.topologyName.equals(this.topologyName.toLowerCase())) {
      LOG.log(Level.SEVERE, "K8s scheduler does not allow upper case topologies");
      return false;
    }

    String deploymentURI = String.format(
        "%s/api/v1/namespaces/default/pods",
        this.kubernetesURI);

    boolean allSuccessful = true;

    for (int i = 0; i < appConfs.length; i++) {
      LOG.log(Level.INFO, "Topology configuration is: " + appConfs[i]);

      // Get a connection
      HttpURLConnection conn = NetworkUtils.getHttpConnection(deploymentURI);
      if (conn == null) {
        LOG.log(Level.SEVERE, "Fauled to find k8s deployment API");
        return false;
      }

      try {
        // send post request with json body for the topology
        if (!NetworkUtils.sendHttpPostRequest(conn,
                                              NetworkUtils.JSON_TYPE,
                                              appConfs[i].getBytes())) {
          LOG.log(Level.SEVERE, "Failed to send post to k8s deployment api");
          allSuccessful = false;
          break;
        }

        // check the response
        boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_CREATED);

        if (success) {
          LOG.log(Level.INFO, "Topology Deployment submitted to k8s deployment API successfully");
        } else {
          LOG.log(Level.SEVERE, "Failed to submit Deployment to k8s");
          byte[] bytes = NetworkUtils.readHttpResponse(conn);
          LOG.log(Level.INFO, Arrays.toString(bytes));
          allSuccessful = false;
          break;
        }
      } finally {
        conn.disconnect();
      }

    }


    return allSuccessful;
  }




}
