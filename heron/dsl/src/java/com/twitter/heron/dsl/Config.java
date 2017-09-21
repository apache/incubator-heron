//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.dsl;

import java.io.Serializable;

import com.twitter.heron.common.basics.ByteAmount;

/**
 * Config is the way users configure the execution of the topology.
 * Things like tuple delivery semantics, resources used, as well as
 * user defined key/value pairs are passed on to the runner via
 * this class.
 */
public final class Config implements Serializable {
  private static final long serialVersionUID = 6204498077403076352L;
  private com.twitter.heron.api.Config heronConfig;
  enum DeliverySemantics {
    ATMOST_ONCE,
    ATLEAST_ONCE,
    EFFECTIVELY_ONCE
  }

  public Config() {
    heronConfig = new com.twitter.heron.api.Config();
  }

  Config(com.twitter.heron.api.Config config) {
    heronConfig = config;
  }

  com.twitter.heron.api.Config getHeronConfig() {
    return heronConfig;
  }

  /**
   * Sets the delivery semantics of the topology
   * @param semantic The delivery semantic to be enforced
   */
  public void setDeliverySemantics(DeliverySemantics semantic) {
    heronConfig.setTopologyReliabilityMode(translateSemantics(semantic));
  }

  /**
   * Sets the number of containers to run this topology
   * @param numContainers The number of containers to distribute this topology
   */
  public void setNumContainers(int numContainers) {
    heronConfig.setNumStmgrs(numContainers);
  }

  /**
   * Sets resources used per container by this topology
   * @param resource The resource per container to dedicate per container
   */
  public void setContainerResources(Resources resource) {
    heronConfig.setContainerCpuRequested(resource.getCpu());
    heronConfig.setContainerRamRequested(ByteAmount.fromBytes(resource.getRam()));
  }

  /**
   * Sets some user defined key value mapping
   * @param key The user defined key
   * @param value The user defined object
   */
  public void setUserConfig(String key, Object value) {
    heronConfig.put(key, value);
  }

  /**
   * Fetches the user defined key value mapping
   * @param key The user defined key
   * @return Any object the user stored or null if nothing was stored for this key
   */
  public Object getUserConfig(String key) {
    return heronConfig.get(key);
  }

  private com.twitter.heron.api.Config.TopologyReliabilityMode translateSemantics(
      DeliverySemantics semantics) {
    switch (semantics) {
      case ATMOST_ONCE:
        return com.twitter.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
      case ATLEAST_ONCE:
        return com.twitter.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;
      case EFFECTIVELY_ONCE:
        return com.twitter.heron.api.Config.TopologyReliabilityMode.EFFECTIVELY_ONCE;
      default:
        return com.twitter.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
    }
  }
}
