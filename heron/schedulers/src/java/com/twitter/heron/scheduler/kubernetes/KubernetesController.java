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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import com.twitter.heron.scheduler.TopologySubmissionException;
import com.twitter.heron.scheduler.utils.SchedulerJsonAPIUtils;

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
    SchedulerJsonAPIUtils.schedulerDeleteRequest(deploymentURI, HttpURLConnection.HTTP_OK);
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

    return SchedulerJsonAPIUtils.schedulerGetRequest(podURI, HttpURLConnection.HTTP_OK);
  }

  /**
   * Deploy a single container (Pod)
   *
   * @param deployConf, the json body as a string
   */
  protected void deployContainer(String deployConf) {
    String deploymentURI = String.format(
        "%s/api/v1/namespaces/%s/pods",
        this.kubernetesURI,
        this.kubernetesNamespace);

    SchedulerJsonAPIUtils.schedulerPostRequest(deploymentURI, deployConf, HttpURLConnection.HTTP_OK);
  }

  /**
   * Remove a single container (Pod)
   * @param podId, the pod id (<topology_name>-<container_index>
   */
  protected void removeContainer(String podId) {
    String podURI = String.format(
        "%s/api/v1/namespaces/%s/pods/%s",
        this.kubernetesURI,
        this.kubernetesNamespace,
        podId);

    SchedulerJsonAPIUtils.schedulerDeleteRequest(podURI, HttpURLConnection.HTTP_OK);
  }

  /**
   * Restart the topology (current unimplemented)
   * @param appId, id of the topology
   * @return
   */
  protected boolean restartApp(int appId) {
    String message = "Restarting the whole topology is not supported yet. "
        + "Please kill and resubmit the topology.";
    LOG.log(Level.SEVERE, message);
    return false;
  }

  /**
   * Submit a topology to kubernetes based on a set of pod configurations
   *
   * @return success
   */
  protected boolean submitTopology(String[] appConfs) {

    if (!this.topologyName.equals(this.topologyName.toLowerCase())) {
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topologies.");
    }

    for (String appConf : appConfs) {
      deployContainer(appConf);
    }
    return true;
  }

}
