/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.kubernetes;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.squareup.okhttp.Response;

import org.apache.heron.scheduler.TopologyRuntimeManagementException;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;

public class KubernetesCompat {

  private static final Logger LOG = Logger.getLogger(KubernetesCompat.class.getName());

  boolean killTopology(String kubernetesUri, String topology, String namespace) {
    final CoreV1Api client = new CoreV1Api(new ApiClient().setBasePath(kubernetesUri));

    // old version deployed topologies as naked pods
    try {
      final String labelSelector = KubernetesConstants.LABEL_TOPOLOGY + "=" + topology;
      final Response response =
          client.deleteCollectionNamespacedPodCall(namespace, null, null, null, null,
          labelSelector, null, null, null, null, null, null)
          .execute();
      if (!response.isSuccessful()) {
        LOG.log(Level.SEVERE, "Error killing topology message: " + response.message());
        KubernetesUtils.logResponseBodyIfPresent(LOG, response);

        throw new TopologyRuntimeManagementException(
            KubernetesUtils.errorMessageFromResponse(response));
      }
    } catch (IOException | ApiException e) {
      LOG.log(Level.SEVERE, "Error killing topology " + e.getMessage());
      if (e instanceof ApiException) {
        LOG.log(Level.SEVERE, "Error details:\n" +  ((ApiException) e).getResponseBody());
      }
      return false;
    }

    return true;
  }
}
