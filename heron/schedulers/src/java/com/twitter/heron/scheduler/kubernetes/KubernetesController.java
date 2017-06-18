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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
  protected boolean killTopology() {

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

  /**
   * Get information about a pod
   *
   * @return json object for pod
   */
  protected JsonNode getBasePod(String podId) {

    String podURI = String.format(
        "%s/api/v1/namespaces/default/pods/%s",
        this.kubernetesURI,
        podId);

    // Get a connection
    HttpURLConnection conn = NetworkUtils.getHttpConnection(podURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find k8s deployment API");
      return null;
    }

    try {
      // send get request
      if (!NetworkUtils.sendHttpGetRequest(conn)) {
        LOG.log(Level.SEVERE, "Failed to send GET to k8s deployment API");
        return null;
      }

      // check the response
      boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_OK);

      if (success) {
        LOG.log(Level.INFO, "Pulled existing pod from k8s");
        byte[] bytes = NetworkUtils.readHttpResponse(conn);

        // read the pod definition
        ObjectMapper mapper = new ObjectMapper();
        JsonNode podDefinition = mapper.readTree(bytes);
        return podDefinition;
      } else {
        LOG.log(Level.SEVERE, "Failure to receive existing pod from k8s");
        return null;
      }
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Unable to parse pod JSON");
      return null;
    } finally {
      conn.disconnect();
    }
  }

  protected boolean deployContainer(String deployConf) {
    String deploymentURI = String.format(
        "%s/api/v1/namespaces/default/pods",
        this.kubernetesURI);

    // Get a connection
    HttpURLConnection conn = NetworkUtils.getHttpConnection(deploymentURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find k8s deployment API");
      return false;
    }

    try {
      // send post request with json body for the topology
      if (!NetworkUtils.sendHttpPostRequest(conn,
          NetworkUtils.JSON_TYPE,
          deployConf.getBytes())) {
        LOG.log(Level.SEVERE, "Failed to send post to k8s deployment api");
        return false;
      }

      // check the response
      boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_CREATED);

      if (success) {
        LOG.log(Level.INFO, "Topology Deployment submitted to k8s deployment API successfully");
      } else {
        LOG.log(Level.SEVERE, "Failed to submit Deployment to k8s");
        byte[] bytes = NetworkUtils.readHttpResponse(conn);
        LOG.log(Level.INFO, Arrays.toString(bytes));
        return false;
      }
    } finally {
      conn.disconnect();
    }
    return true;
  }

  protected boolean removeContainer(String podId) {
    String podURI = String.format(
        "%s/api/v1/namespaces/default/pods/%s",
        this.kubernetesURI,
        podId);

    // Get a connection
    HttpURLConnection conn = NetworkUtils.getHttpConnection(podURI);
    if (conn == null) {
      LOG.log(Level.SEVERE, "Failed to find k8s deployment API");
      return false;
    }

    try {
      // send get request
      if (!NetworkUtils.sendHttpDeleteRequest(conn)) {
        LOG.log(Level.SEVERE, "Failed to send DELETE to k8s deployment API");
        return false;
      }

      // check the response
      boolean success = NetworkUtils.checkHttpResponseCode(conn, HttpURLConnection.HTTP_OK);

      if (success) {
        LOG.log(Level.INFO, "Deleted existing pod from k8s");
        return true;
      } else {
        LOG.log(Level.SEVERE, "Failure to delete existing pod from k8s");
        return false;
      }
    } finally {
      conn.disconnect();
    }
  }

  protected boolean restartApp(int appId) {
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
  protected boolean submitTopology(String[] appConfs) {

    if (!this.topologyName.equals(this.topologyName.toLowerCase())) {
      LOG.log(Level.SEVERE, "K8s scheduler does not allow upper case topologies");
      return false;
    }

    String deploymentURI = String.format(
        "%s/api/v1/namespaces/default/pods",
        this.kubernetesURI);

    boolean allSuccessful = true;

    for (int i = 0; i < appConfs.length; i++) {

      allSuccessful = deployContainer(appConfs[i]);
      if (!allSuccessful) {
        break;
      }
    }

    return allSuccessful;
  }

}
