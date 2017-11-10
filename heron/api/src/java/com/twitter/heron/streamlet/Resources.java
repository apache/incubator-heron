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

/**
 * Resources needed by the topology are encapsulated in this class.
 * Currently we deal with CPU and RAM. Others can be added later.
 */
public final class Resources implements Serializable {
  private static final long serialVersionUID = 630451253428388496L;
  private static float CPU_DEFAULT = 1.0f;
  private static long RAM_DEFAULT = 104857600;
  private float cpu;
  private long ram;

  private Resources(Builder builder) {
    this.cpu = builder.cpu;
    this.ram = builder.ram;
  }

  public static Resources defaultResources() {
    return new Builder()
        .build();
  }

  public float getCpu() {
    return cpu;
  }

  public long getRam() {
    return ram;
  }

  @Override
  public String toString() {
    return String.format("{ CPU: %s RAM: %s }", String.valueOf(cpu), String.valueOf(ram));
  }

  public static class Builder {
    private float cpu;
    private long ram;

    public Builder() {
      this.cpu = CPU_DEFAULT;
      this.ram = RAM_DEFAULT;
    }

    /**
     * Sets the RAM to be used by the topology (in megabytes)
     * @param nram The number of megabytes of RAM
     */
    public Builder setRamInMB(long nram) {
      this.ram = nram * 1024;
      return this;
    }

    /**
     * Sets the RAM to be used by the topology (in gigabytes)
     * @param nram The number of gigabytes of RAM
     */
    public Builder setRamInGB(long nram) {
      this.ram = nram * 1024 * 1024;
      return this;
    }

    /**
     * Sets the total number of CPUs to be used by the topology
     * @param cpu The number of CPUs (as a float)
     */
    public Builder setCpu(float cpu) {
      this.cpu = cpu;
      return this;
    }

    /**
     * Sets the RAM to be used by the topology (in bytes)
     * @param ram The number of bytes of RAM
     */
    public Builder setRam(long ram) {
      this.ram = ram;
      return this;
    }

    public Resources build() {
      return new Resources(this);
    }
  }
}
