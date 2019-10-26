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


package org.apache.heron.streamlet;

import java.io.Serializable;

import org.apache.heron.common.basics.ByteAmount;

/**
 * Config is the way users configure the execution of the topology.
 * Things like streamlet delivery semantics, resources used, as well as
 * user-defined key/value pairs are passed on to the topology runner via
 * this class.
 */
public final class Config implements Serializable {
  private static final long serialVersionUID = 6204498077403076352L;

  private org.apache.heron.api.Config heronConfig;
  private final double cpu;
  private final ByteAmount ram;
  private final DeliverySemantics deliverySemantics;
  private final Serializer serializer;
  private static final long MB = 1024 * 1024;
  private static final long GB = 1024 * MB;

  /**
   * An enum encapsulating the delivery semantics that can be applied to Heron topologies. The
   * options are currently: at most once, at least once, or effectively once.
   */
  public enum DeliverySemantics {
    ATMOST_ONCE,
    ATLEAST_ONCE,
    EFFECTIVELY_ONCE
  }

  /**
   * An enum encapsulating the serializers that can be used for data in the topology. The options
   * are currently: the Kryo serializer or the native Java serializer.
   */
  public enum Serializer {
    JAVA,
    KRYO
  }

  private static class Defaults {
    static final double CPU = -1.0;                             // -1 means undefined
    static final ByteAmount RAM = ByteAmount.fromBytes(-1);     // -1 means undefined
    static final DeliverySemantics SEMANTICS = DeliverySemantics.ATMOST_ONCE;
    static final Serializer SERIALIZER = Serializer.KRYO;
  }

  private Config(Builder builder) {
    heronConfig = builder.config;
    cpu = builder.cpu;
    ram = builder.ram;
    serializer = builder.serializer;
    deliverySemantics = builder.deliverySemantics;
  }

  /**
   * Sets the topology to use the default configuration: 100 megabytes of RAM per container, 1.0
   * CPUs per container, at-most-once delivery semantics, and the Kryo serializer.
   */
  public static Config defaultConfig() {
    return new Builder()
        .build();
  }

  /**
   * Returns a new {@link Builder} that can be used to create a configuration object for Streamlet
   * API topologies
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public org.apache.heron.api.Config getHeronConfig() {
    return heronConfig;
  }

  /**
   * Gets the CPU used per topology container
   * @return the per-container CPU as a double
   */
  public double getPerContainerCpu() {
    return cpu;
  }

  /**
   * Gets the RAM used per topology container as a number of bytes
   * @return the per-container RAM in bytes
   */
  public long getPerContainerRam() {
    return getPerContainerRamAsBytes();
  }

  /**
   * Gets the RAM used per topology container as a number of bytes
   * @return the per-container RAM in bytes
   */
  public long getPerContainerRamAsBytes() {
    return ram.asBytes();
  }

  /**
   * Gets the RAM used per topology container as a number of megabytes
   * @return the per-container RAM in megabytes
   */
  public long getPerContainerRamAsMegabytes() {
    return ram.asMegabytes();
  }

  /**
   * Gets the RAM used per topology container as a number of gigabytes
   * @return the per-container RAM in gigabytes
   */
  public long getPerContainerRamAsGigabytes() {
    return ram.asGigabytes();
  }

  /**
   * Gets the delivery semantics applied to the topology
   * @return the delivery semantics as an enum
   */
  public DeliverySemantics getDeliverySemantics() {
    return deliverySemantics;
  }

  /**
   * Gets the serializer used by the topology
   * @return the serializer as an enum
   */
  public Serializer getSerializer() {
    return serializer;
  }

  private static org.apache.heron.api.Config.TopologyReliabilityMode translateSemantics(
      DeliverySemantics semantics) {
    switch (semantics) {
      case ATMOST_ONCE:
        return org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
      case ATLEAST_ONCE:
        return org.apache.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;
      case EFFECTIVELY_ONCE:
        return org.apache.heron.api.Config.TopologyReliabilityMode.EFFECTIVELY_ONCE;
      default:
        return org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
    }
  }

  private static String translateSerializer(Serializer serializer) {
    switch (serializer) {
      case JAVA:
        return org.apache.heron.api.serializer.JavaSerializer.class.getName();
      case KRYO:
        return org.apache.heron.api.serializer.KryoSerializer.class.getName();
      default:
        // KryoSerializer is the default in Streamlet API
        return org.apache.heron.api.serializer.KryoSerializer.class.getName();
    }
  }

  public static final class Builder {
    private org.apache.heron.api.Config config;
    private double cpu;
    private ByteAmount ram;
    private DeliverySemantics deliverySemantics;
    private Serializer serializer;

    private Builder() {
      config = new org.apache.heron.api.Config();
      cpu = Defaults.CPU;
      ram = Defaults.RAM;
      deliverySemantics = Defaults.SEMANTICS;
      serializer = Defaults.SERIALIZER;
    }

    /**
     * Sets the per-container (per-instance) CPU to be used by this topology
     * @param perContainerCpu Per-container (per-instance) CPU as a double
     */
    public Builder setPerContainerCpu(double perContainerCpu) {
      this.cpu = perContainerCpu;
      // Different packing algorithm might use different configs. Set all of them here.
      config.setContainerCpuRequested(perContainerCpu);
      config.setContainerMaxCpuHint(perContainerCpu);
      return this;
    }

    /**
     * Sets the per-container (per-instance) RAM to be used by this topology
     * @param perContainerRam Per-container (per-instance) RAM expressed as a Long.
     */
    public Builder setPerContainerRam(long perContainerRam) {
      return setPerContainerRamInBytes(perContainerRam);
    }

    /**
     * Sets the per-container (per-instance) RAM to be used by this topology as a number of bytes
     * @param perContainerRam Per-container (per-instance) RAM expressed as a Long.
     */
    public Builder setPerContainerRamInBytes(long perContainerRam) {
      this.ram = ByteAmount.fromBytes(perContainerRam);
      // Different packing algorithm might use different configs. Set all of them here.
      config.setContainerRamRequested(ram);
      config.setContainerMaxRamHint(ram);
      return this;
    }

    /**
     * Sets the per-container (per-instance) RAM to be used by this topology in megabytes
     * @param perContainerRamMB Per-container (per-instance) RAM expressed as a Long.
     */
    public Builder setPerContainerRamInMegabytes(long perContainerRamMB) {
      return setPerContainerRam(perContainerRamMB * MB);
    }

    /**
     * Sets the per-container (per-instance) RAM to be used by this topology in gigabytes
     * @param perContainerRamGB Per-container (per-instance) RAM expressed as a Long.
     */
    public Builder setPerContainerRamInGigabytes(long perContainerRamGB) {
      return setPerContainerRam(perContainerRamGB * GB);
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
      return this;
    }

    private void applyDeliverySemantics() {
      config.setTopologyReliabilityMode(Config.translateSemantics(deliverySemantics));
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
     * Sets the {@link Serializer} to be used by the topology (current options are {@link
     * KryoSerializer} and the native Java serializer.
     * @param topologySerializer The data serializer to use for streamlet elements in the topology.
     */
    public Builder setSerializer(Serializer topologySerializer) {
      this.serializer = topologySerializer;
      return this;
    }

    private void applySerializer() {
      try {
        String serializerClass = translateSerializer(serializer);
        config.setSerializationClassName(serializerClass);
      } catch (NoClassDefFoundError e) {
        throw new RuntimeException("Linking with serializer" + serializer + " is needed");
      }
    }

    public Config build() {
      // Translate and apply Streamlet configs to Heron configs
      applySerializer();
      applyDeliverySemantics();

      return new Config(this);
    }
  }
}
