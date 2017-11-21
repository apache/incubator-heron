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
  private final float cpu;
  private final ByteAmount ram;
  private final DeliverySemantics deliverySemantics;
  private final Serializer serializer;

  private com.twitter.heron.api.Config heronConfig;

  public enum DeliverySemantics {
    ATMOST_ONCE,
    ATLEAST_ONCE,
    EFFECTIVELY_ONCE
  }

  public enum Serializer {
    JAVA,
    KRYO
  }

  private static class Defaults {
    static final boolean USE_KRYO = true;
    static final com.twitter.heron.api.Config CONFIG = new com.twitter.heron.api.Config();
    static final float CPU = 1.0f;
    static final ByteAmount RAM = ByteAmount.fromMegabytes(100);
    static final DeliverySemantics SEMANTICS = DeliverySemantics.ATMOST_ONCE;
    static final Serializer SERIALIZER = Serializer.KRYO;
  }

  private Config(Builder builder) {
    serializer = builder.serializer;
    heronConfig = builder.config;
    cpu = builder.cpu;
    ram = builder.ram;
    deliverySemantics = builder.deliverySemantics;
  }

  public static Config defaultConfig() {
    return new Builder()
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  com.twitter.heron.api.Config getHeronConfig() {
    return heronConfig;
  }

  public float getPerContainerCpu() {
    return cpu;
  }

  public ByteAmount getPerContainerRam() {
    return ram;
  }

  public DeliverySemantics getDeliverySemantics() {
    return deliverySemantics;
  }

  public Serializer getSerializer() {
    return serializer;
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
    private float cpu;
    private ByteAmount ram;
    private DeliverySemantics deliverySemantics;
    private Serializer serializer;

    private Builder() {
      config = Defaults.CONFIG;
      cpu = Defaults.CPU;
      ram = Defaults.RAM;
      deliverySemantics = Defaults.SEMANTICS;
      serializer = Serializer.KRYO;
    }

    /**
     * Sets the per-container (per-instance) CPU to be used by this topology
     * @param cpu Per-container (per-instance) CPU as a float
     */
    public Builder setPerContainerCpu(float cpu) {
      this.cpu = cpu;
      return this;
    }

    /**
     * Sets the per-container (per-instance) RAM to be used by this topology
     * @param ram Per-container (per-instance) RAM expressed as a {@link ByteAmount}
     */
    public Builder setPerContainerRam(ByteAmount ram) {
      this.ram = ram;
      return this;
    }

    /**
     * Sets the number of containers to run this topology
     * @param numContainers The number of containers across which to distribute this topology
     */
    public Builder setNumContainers(int numContainers) {
      config.setNumStmgrs(numContainers);
      return this;
    }

    /**
     * Sets the delivery semantics of the topology
     * @param semantics The delivery semantic to be enforced
     */
    public Builder setDeliverySemantics(DeliverySemantics semantics) {
      this.deliverySemantics = semantics;
      config.setTopologyReliabilityMode(Config.translateSemantics(semantics));
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

    private void useKryo() {
      try {
        config.setSerializationClassName(KryoSerializer.class.getName());
      } catch (NoClassDefFoundError e) {
        throw new RuntimeException("Linking with kryo is needed because useKryoSerializer is used");
      }
    }

    public Builder setSerializer(Serializer serializer) {
      this.serializer = serializer;
      if (serializer.equals(Serializer.KRYO)) {
        useKryo();
      }
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }
}
