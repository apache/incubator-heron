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

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.system.PhysicalPlans.PhysicalPlan;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.NetworkUtils;

import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_TOPOLOGY_NAME;

/**
 * A topology's physical plan may get updated at runtime. This provider is used to
 * fetch the latest version from the tmanager and provide to any dependent components.
 */
public class PhysicalPlanProvider implements Provider<PhysicalPlan> {
  private static final Logger LOG = Logger.getLogger(PhysicalPlanProvider.class.getName());

  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  // Cache the physical plan between two successful get() invocations.
  private PhysicalPlan physicalPlan = null;

  @Inject
  public PhysicalPlanProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                              @Named(CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
  }

  protected PhysicalPlan ParseResponseToPhysicalPlan(byte[] responseData) {
    // byte to base64 string
    String encodedString = new String(responseData);
    LOG.fine("tmanager returns physical plan in base64 str: " + encodedString);
    // base64 string to proto bytes
    byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
    // construct proto obj from bytes
    PhysicalPlan pp = null;
    try {
      pp = PhysicalPlan.parseFrom(decodedBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new InvalidStateException(topologyName, "Failed to fetch the physical plan");
    }
    return pp;
  }

  @Override
  public synchronized PhysicalPlan get() {
    TopologyManager.TManagerLocation tManagerLocation
        = stateManagerAdaptor.getTManagerLocation(topologyName);
    String host = tManagerLocation.getHost();
    int port = tManagerLocation.getControllerPort();

    // construct metric cache stat url
    String url = "http://" + host + ":" + port + "/get_current_physical_plan";
    LOG.fine("tmanager physical plan query endpoint: " + url);

    // http communication
    HttpURLConnection con = NetworkUtils.getHttpConnection(url);
    NetworkUtils.sendHttpGetRequest(con);
    byte[] responseData = NetworkUtils.readHttpResponse(con);

    physicalPlan = ParseResponseToPhysicalPlan(responseData);
    return physicalPlan;
  }

  /**
   * try best effort to return a latest physical plan
   * 1. refresh physical plan
   * 2. if refreshing fails, return the last physical plan
   * @return physical plan
   */
  public PhysicalPlan getPhysicalPlan() {
    try {
      get();
    } catch (InvalidStateException e) {
      if (physicalPlan == null) {
        throw e;
      }
    }
    return physicalPlan;
  }

  /**
   * A utility method to extract bolt component names from the topology.
   *
   * @return list of all bolt names
   */
  public List<String> getBoltNames(PhysicalPlan pp) {
    TopologyAPI.Topology localTopology = pp.getTopology();
    ArrayList<String> boltNames = new ArrayList<>();
    for (TopologyAPI.Bolt bolt : localTopology.getBoltsList()) {
      boltNames.add(bolt.getComp().getName());
    }

    return boltNames;
  }

  public List<String> getBoltNames() {
    PhysicalPlan pp = getPhysicalPlan();
    return getBoltNames(pp);
  }

  /**
   * A utility method to extract spout component names from the topology.
   *
   * @return list of all spout names
   */
  public List<String> getSpoutNames(PhysicalPlan pp) {
    TopologyAPI.Topology localTopology = pp.getTopology();
    ArrayList<String> spoutNames = new ArrayList<>();
    for (TopologyAPI.Spout spout : localTopology.getSpoutsList()) {
      spoutNames.add(spout.getComp().getName());
    }

    return spoutNames;
  }

  public List<String> getSpoutNames() {
    PhysicalPlan pp = getPhysicalPlan();
    return getSpoutNames(pp);
  }

}
