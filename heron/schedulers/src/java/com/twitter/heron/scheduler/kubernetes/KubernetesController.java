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

import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.TopologySubmissionException;
import com.twitter.heron.spi.utils.NetworkUtils;

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private final String kubernetesURI;
  private final String kubernetesNamespace;
  private final String topologyName;
  private final boolean isVerbose;

  public KubernetesController(
      String kubernetesURI,
      String kubernetesNamespace,
      String topologyName,
      boolean isVerbose
  ) {
    this.kubernetesURI = kubernetesURI;
    this.kubernetesNamespace = kubernetesNamespace;
    this.topologyName = topologyName;
    this.isVerbose = isVerbose;
  }

  private HttpURLConnection getUrlConnection(String uri) {
    HttpURLConnection conn = NetworkUtils.getHttpConnection(uri);
    if (conn == null) {
      throw new TopologyRuntimeManagementException("Failed to initialize connection to " + uri);
    }
    return conn;
  }

  private JsonNode schedulerGetRequest(String uri, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);
    byte[] responseData;
    try {
      if (!NetworkUtils.sendHttpGetRequest(conn)) {
        throw new TopologyRuntimeManagementException("Failed to send delete request to " + uri);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received " + NetworkUtils.getHttpResponseCode(conn));
      }

      responseData = NetworkUtils.readHttpResponse(conn);

    } finally {
      conn.disconnect();
    }

    // parse of the json
    JsonNode podDefinition;

    try {
      // read the json data
      ObjectMapper mapper = new ObjectMapper();
      podDefinition = mapper.readTree(responseData);
    } catch (IOException ioe) {
      throw new TopologyRuntimeManagementException("Failed to parse JSON response from "
          + "Scheduler API");
    }

    return podDefinition;
  }

  private void schedulerDeleteRequest(String uri, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);
    try {
      if (!NetworkUtils.sendHttpDeleteRequest(conn)) {
        throw new TopologyRuntimeManagementException("Failed to send delete request to " + uri);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received "
            + NetworkUtils.getHttpResponseCode(conn));
      }


    } finally {
      conn.disconnect();
    }
  }

  private void schedulerPostRequest(String uri, String jsonBody, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);

    try {
      // send post request with json body for the topology
      if (!NetworkUtils.sendHttpPostRequest(conn,
          NetworkUtils.JSON_TYPE,
          jsonBody.getBytes())) {
        throw new TopologyRuntimeManagementException("Failed to send POST to " + uri);
      }

      // check the response
      if(NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        LOG.log(Level.INFO, "Topology submitted to scheduler API");
      } else {
        byte[] bytes = NetworkUtils.readHttpResponse(conn);
        LOG.log(Level.SEVERE, "Failed to send POST request to scheduler");
        LOG.log(Level.SEVERE, Arrays.toString(bytes));
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received "
            + NetworkUtils.getHttpResponseCode(conn));
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Kill a topology in kubernetes based on a configuration
   *
   * @return success
   */
  protected boolean killTopology() {

    // Setup connection
    String deploymentURI = String.format(
        "%s/api/v1/namespaces/%s/pods?labelSelector=topology%%3D%s",
        this.kubernetesURI,
        this.kubernetesNamespace,
        this.topologyName);

    // send the delete request to the scheduler
    schedulerDeleteRequest(deploymentURI, HttpURLConnection.HTTP_OK);
    return true;
  }

  /**
   * Get information about a pod
   *
   * @return json object for pod
   */
  protected JsonNode getBasePod(String podId) {

    String podURI = String.format(
        "%s/api/v1/namespaces/%s/pods/%s",
        this.kubernetesURI,
        this.kubernetesNamespace,
        podId);

    return schedulerGetRequest(podURI, HttpURLConnection.HTTP_OK);
  }

  protected boolean deployContainer(String deployConf) {
    String deploymentURI = String.format(
        "%s/api/v1/namespaces/%s/pods",
        this.kubernetesURI,
        this.kubernetesNamespace);

    schedulerPostRequest(deploymentURI, deployConf, HttpURLConnection.HTTP_OK);
    return true;
  }

  protected boolean removeContainer(String podId) {
    String podURI = String.format(
        "%s/api/v1/namespaces/%s/pods/%s",
        this.kubernetesURI,
        this.kubernetesNamespace,
        podId);

    schedulerDeleteRequest(podURI, HttpURLConnection.HTTP_OK);
    return true;
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
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topologies.");
    }

    for (String appConf : appConfs) {
      if(!deployContainer(appConf)) {
        return false;
      }
    }
    return true;
  }

}
