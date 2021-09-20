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


import java.util.Optional;

import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;
import org.apache.heron.spi.scheduler.IScalable;

public abstract class KubernetesController implements IScalable {

  private final Config configuration;
  private final Config runtimeConfiguration;
  private String namespace;

  KubernetesController(Config configuration, Config runtimeConfiguration) {
    this.configuration = configuration;
    this.runtimeConfiguration = runtimeConfiguration;
    namespace = Optional.ofNullable(KubernetesContext.getKubernetesNamespace(configuration))
        .orElse(KubernetesConstants.DEFAULT_NAMESPACE);
  }

  Config getConfiguration() {
    return configuration;
  }

  Config getRuntimeConfiguration() {
    return runtimeConfiguration;
  }

  String getNamespace() {
    return namespace;
  }

  String getTopologyName() {
    return Runtime.topologyName(runtimeConfiguration);
  }

  String getKubernetesUri() {
    return KubernetesContext.getSchedulerURI(configuration);
  }

  Resource getContainerResource(PackingPlan packingPlan) {
    // Align resources to maximal requested resource
    PackingPlan updatedPackingPlan = packingPlan.cloneWithHomogeneousScheduledResource();
    SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtimeConfiguration),
        updatedPackingPlan, Runtime.schedulerStateManagerAdaptor(runtimeConfiguration));

    return updatedPackingPlan.getContainers().iterator().next().getScheduledResource().get();
  }

  abstract boolean submit(PackingPlan packingPlan);

  abstract boolean killTopology();

  abstract boolean restart(int shardId);

}
