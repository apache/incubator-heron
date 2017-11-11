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

package com.twitter.heron.scheduler.ecs;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import com.twitter.heron.scheduler.TopologySubmissionException;
import com.twitter.heron.scheduler.utils.HttpJsonClient;

public class EcsController {
  private static final Logger LOG = Logger.getLogger(EcsController.class.getName());

  private final String topologyName;
  private final String baseUriPath;
  private final boolean isVerbose;

  public EcsController(String kubernetesURI, String kubernetesNamespace,
                               String topologyName, boolean isVerbose) {

    if (kubernetesNamespace == null) {
      this.baseUriPath = String.format("%s/api/v1/namespaces/default/pods", kubernetesURI);
    } else {
      this.baseUriPath = String.format("%s/api/v1/namespaces/%s/pods", kubernetesURI,
          kubernetesNamespace);
    }
    this.topologyName = topologyName;
    this.isVerbose = isVerbose;
  }

  /**
   * Kill a topology in kubernetes based on a configuration
   */
  protected boolean killTopology() {

    // Setup connection
    String deploymentURI = String.format(
        "%s?labelSelector=topology%%3D%s",
        this.baseUriPath,
        this.topologyName);

    // send the delete request to the scheduler
    HttpJsonClient jsonAPIClient = new HttpJsonClient(deploymentURI);
    try {
      jsonAPIClient.delete(HttpURLConnection.HTTP_OK);
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Problem sending delete request: " + deploymentURI, ioe);
      return false;
    }
    return true;
  }

  /**
   * Get information about a pod
   */
  protected JsonNode getBasePod(String podId) throws IOException {

    String podURI = String.format(
        "%s/%s",
        this.baseUriPath,
        podId);

    // send the delete request to the scheduler
    HttpJsonClient jsonAPIClient = new HttpJsonClient(podURI);
    JsonNode result;
    try {
      result = jsonAPIClient.get(HttpURLConnection.HTTP_OK);
    } catch (IOException ioe) {
      throw ioe;
    }
    return result;
  }

  /**
   * Deploy a single container (Pod)
   *
   * @param deployConf, the json body as a string
   */
  protected void deployContainer(String deployConf) throws IOException {
    HttpJsonClient jsonAPIClient = new HttpJsonClient(this.baseUriPath);
    jsonAPIClient.post(deployConf, HttpURLConnection.HTTP_CREATED);
  }

  /**
   * Remove a single container (Pod)
   *
   * @param podId, the pod id (TOPOLOGY_NAME-CONTAINER_INDEX)
   */
  protected void removeContainer(String podId) throws IOException {
    String podURI = String.format(
        "%s/%s",
        this.baseUriPath,
        podId);

    HttpJsonClient jsonAPIClient = new HttpJsonClient(podURI);
    jsonAPIClient.delete(HttpURLConnection.HTTP_OK);
  }

  /**
   * Restart the topology (current unimplemented)
   * @param appId, id of the topology
   */
  protected boolean restartApp(int appId) {
    String message = "Restarting the whole topology is not supported yet. "
        + "Please kill and resubmit the topology.";
    LOG.log(Level.SEVERE, message);
    return false;
  }

  /**
   * Submit a topology to kubernetes based on a set of pod configurations
   */
  protected boolean submitTopology(String[] appConfs) {
    if (!this.topologyName.equals(this.topologyName.toLowerCase())) {
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topologies.");
    }

    for (String appConf : appConfs) {
      try {
        deployContainer(appConf);
      } catch (IOException ioe) {
        final String message = ioe.getMessage();
        LOG.log(Level.SEVERE, "Problem deploying container: " + ioe.getMessage(), ioe);
        LOG.log(Level.SEVERE, "Container config: " + appConf);
        throw new TopologySubmissionException(message);
      }
    }
    return true;
  }
}
