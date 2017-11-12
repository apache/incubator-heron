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

package com.twitter.heron.streamlet;

import java.io.Serializable;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.impl.KryoSerializer;

/**
 * Config is the way users configure the execution of the topology.
 * Things like streamlet delivery semantics, resources used, as well as
 * user-defined key/value pairs are passed on to the topology runner via
 * this class.
 */
public final class Config implements Serializable {
  private static final long serialVersionUID = 6204498077403076352L;

  private com.twitter.heron.api.Config heronConfig;

  public enum DeliverySemantics {
    ATMOST_ONCE,
    ATLEAST_ONCE,
    EFFECTIVELY_ONCE
  }

  private Config(Builder builder) {
    heronConfig = builder.config;
  }

  public static Config defaultConfig() {
    return new Builder()
        .build();
  }

  com.twitter.heron.api.Config getHeronConfig() {
    return heronConfig;
  }

  private static com.twitter.heron.api.Config.TopologyReliabilityMode translateSemantics(
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

  public static class Builder {
    private com.twitter.heron.api.Config config;

    public Builder() {
      config = new com.twitter.heron.api.Config();
    }

    /**
     * Sets the number of containers to run this topology
     * @param numContainers The number of containers to distribute this topology
     */
    public Builder setNumContainers(int numContainers) {
      config.setNumStmgrs(numContainers);
      return this;
    }

    /**
     * Sets resources used per container by this topology
     * @param resources The resource to dedicate per container
     */
    public Builder setContainerResources(Resources resources) {
      config.setContainerCpuRequested(resources.getCpu());
      config.setContainerRamRequested(ByteAmount.fromBytes(resources.getRam()));
      return this;
    }

    /**
     * Sets the delivery semantics of the topology
     * @param semantic The delivery semantic to be enforced
     */
    public Builder setDeliverySemantics(DeliverySemantics semantic) {
      config.setTopologyReliabilityMode(Config.translateSemantics(semantic));
      return this;
    }

    /**
     * Sets some user-defined key/value mapping
     * @param key The user-defined key
     * @param value The user-defined value
     */
    public Builder setUserConfig(String key, Object value) {
      config.put(key, value);
      return this;
    }

    /**
     * Sets the topology to use the Kryo serializer for serializing
     * streamlet elements
     */
    public Builder useKryoSerializer() {
      try {
        config.setSerializationClassName(new KryoSerializer().getClass().getName());
      } catch (NoClassDefFoundError e) {
        throw new RuntimeException("Linking with kryo is needed because useKryoSerializer is used");
      }
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }
}
