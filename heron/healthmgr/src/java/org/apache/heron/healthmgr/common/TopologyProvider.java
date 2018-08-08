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

package org.apache.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.generated.TopologyAPI.Topology;
import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_TOPOLOGY_NAME;

/**
 * A topology may be updated after initial deployment. This provider is used to provide the latest
 * version to any dependent components.
 */
@Singleton
public class TopologyProvider implements Provider<Topology> {
  private static final Logger LOG = Logger.getLogger(TopologyProvider.class.getName());

  private Topology topology;
  private PhysicalPlanProvider physicalPlanProvider;

  @Inject
  public TopologyProvider(PhysicalPlanProvider physicalPlanProvider) {
    this.physicalPlanProvider = physicalPlanProvider;
  }

  @Override
  public synchronized Topology get() {
    return physicalPlanProvider.get().getTopology();
  }

  /**
   * A utility method to extract bolt component names from the topology.
   *
   * @return array of all bolt names
   */
  public Collection<String> getBoltNames() {
    Topology localTopology = get();
    ArrayList<String> boltNames = new ArrayList<>();
    for (TopologyAPI.Bolt bolt : localTopology.getBoltsList()) {
      boltNames.add(bolt.getComp().getName());
    }

    return boltNames;
  }

  /**
   * A utility method to extract spout component names from the topology.
   *
   * @return array of all spout names
   */
  public Collection<String> getSpoutNames() {
    Topology localTopology = get();
    ArrayList<String> spoutNames = new ArrayList<>();
    for (TopologyAPI.Bolt spout : localTopology.getBoltsList()) {
      spoutNames.add(spout.getComp().getName());
    }

    return spoutNames;
  }
}
