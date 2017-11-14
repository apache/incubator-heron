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

/**
 * Resources needed by the topology are encapsulated in this class.
 * Currently we deal with CPU and RAM. Others can be added later.
 */
public final class Resources implements Serializable {
  private static final long serialVersionUID = 630451253428388496L;
  private float cpu;
  private ByteAmount ram;

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

  public ByteAmount getRam() {
    return ram;
  }

  @Override
  public String toString() {
    return String.format("{ CPU: %s RAM: %s }", String.valueOf(cpu), String.valueOf(ram));
  }

  public static class Builder {
    private float cpu;
    private ByteAmount ram;

    public Builder() {
      this.cpu = 1.0f;
      this.ram = ByteAmount.fromBytes(104857600);
    }

    /**
     * Sets the RAM to be used by the topology (in megabytes)
     * @param nram The number of megabytes of RAM
     */
    public Builder setRamInMB(long nram) {
      this.ram = ByteAmount.fromMegabytes(nram);
      return this;
    }

    /**
     * Sets the RAM to be used by the topology (in gigabytes)
     * @param nram The number of gigabytes of RAM
     */
    public Builder setRamInGB(long nram) {
      this.ram = ByteAmount.fromGigabytes(nram);
      return this;
    }

    /**
     * Sets the total number of CPUs to be used by the topology
     * @param containerCpu The number of CPUs (as a float)
     */
    public Builder setCpu(float containerCpu) {
      this.cpu = containerCpu;
      return this;
    }

    /**
     * Sets the RAM to be used by the topology (in bytes)
     * @param containerRam The number of bytes of RAM
     */
    public Builder setRam(long containerRam) {
      this.ram = ByteAmount.fromBytes(containerRam);
      return this;
    }

    public Resources build() {
      return new Resources(this);
    }
  }
}
