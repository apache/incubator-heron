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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import com.twitter.heron.scheduler.TopologySubmissionException;
import com.twitter.heron.scheduler.utils.HttpJsonClient;

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private final String topologyName;
  private final String baseReplicaSetUriPath;
  private final String basePodURIPath;
  private final boolean isVerbose;

  public KubernetesController(String kubernetesURI, String kubernetesNamespace,
                               String topologyName, boolean isVerbose) {

    if (kubernetesNamespace == null) {
      this.baseReplicaSetUriPath = String.format(
          "%s/apis/extensions/v1beta1/namespaces/default/replicasets",
          kubernetesURI);
      this.basePodURIPath = String.format("%s/api/v1/namespaces/default/pods", kubernetesURI);
    } else {
      this.baseReplicaSetUriPath = String.format(
          "%s/apis/extensions/v1beta1/namespaces/%s/replicasets",
          kubernetesURI, kubernetesNamespace);
      this.basePodURIPath = String.format("%s/api/v1/namespaces/%s/pods", kubernetesURI,
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
    String replicaSetURI = String.format(
        "%s?labelSelector=topology%%3D%s",
        this.baseReplicaSetUriPath,
        this.topologyName);

    // send the delete request to remove the ReplicaSet
    HttpJsonClient jsonAPIClient = new HttpJsonClient(replicaSetURI);
    try {
      jsonAPIClient.delete(HttpURLConnection.HTTP_OK);
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Problem sending delete request: " + replicaSetURI, ioe);
      return false;
    }

    String podURI = String.format(
        "%s?labelSelector=topology%%3D%s",
        this.basePodURIPath,
        this.topologyName);

    // send the delete request to remove the Pods
    jsonAPIClient = new HttpJsonClient(podURI);
    try {
      jsonAPIClient.delete(HttpURLConnection.HTTP_OK);
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Problem sending delete request: " + podURI, ioe);
      return false;
    }

    // send the delete request to remove the Pods
    return true;
  }

  /**
   * Get information about a Replica Set
   */
  protected JsonNode getBaseReplicaSet(String replicaSetId) throws IOException {

    String replicaSetURI = String.format(
        "%s/%s",
        this.baseReplicaSetUriPath,
        replicaSetId);

    // send the delete request to the scheduler
    HttpJsonClient jsonAPIClient = new HttpJsonClient(replicaSetURI);
    JsonNode result;
    try {
      result = jsonAPIClient.get(HttpURLConnection.HTTP_OK);
    } catch (IOException ioe) {
      throw ioe;
    }
    return result;
  }

  /**
   * Deploy a single instance (Replica Set). Kubernetes will create a Replica Set, as well as
   * the needed Pods that are defined in the spec of the Replica Set.
   *
   * @param deployConf, the json body as a string
   */
  protected void deployContainer(String deployConf) throws IOException {

    HttpJsonClient jsonAPIClient = new HttpJsonClient(this.baseReplicaSetUriPath);
    try {
      jsonAPIClient.post(deployConf, HttpURLConnection.HTTP_CREATED);
    } catch (IOException ioe) {
      throw ioe;
    }

  }

  /**
   * Remove a single container (Replica Set)
   *
   * @param replicaSetId, the Replica Set id (TOPOLOGY_NAME-CONTAINER_INDEX)
   */
  protected void removeContainer(String replicaSetId) throws IOException {
    String replicaSetURI = String.format(
        "%s/%s",
        this.baseReplicaSetUriPath,
        replicaSetId);

    // Delete the replica set
    HttpJsonClient jsonAPIClient = new HttpJsonClient(replicaSetURI);
    jsonAPIClient.delete(HttpURLConnection.HTTP_OK);

    // Delete the pod
    String podURI = String.format(
        "%s/%s",
        this.basePodURIPath,
        replicaSetId);

    // Delete the replica set
    jsonAPIClient = new HttpJsonClient(podURI);
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
   * Submit a topology to kubernetes based on a set of replication controller configurations
   */
  protected boolean submitTopology(String[] appConfs) {

    if (!this.topologyName.equals(this.topologyName.toLowerCase())) {
      throw new TopologySubmissionException("K8S scheduler does not allow upper case topologies.");
    }

    for (String appConf : appConfs) {
      try {
        deployContainer(appConf);
      } catch (IOException ioe) {
        LOG.log(Level.SEVERE, "Problem deploying container with config: " + appConf);
        return false;
      }
    }
    return true;
  }

}
