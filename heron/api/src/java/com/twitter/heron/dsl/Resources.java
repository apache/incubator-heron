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

/**
 * Resources needed by the topology are encapsulated in this class.
 * Currently we deal with cpu and ram. Others can be added later.
 */
public final class Resources implements Serializable {
  private static final long serialVersionUID = 630451253428388496L;
  private float cpu;
  private long ram;

  public float getCpu() {
    return cpu;
  }

  public long getRam() {
    return ram;
  }

  public Resources() {
    this.cpu = 1.0f;
    this.ram = 104857600;
  }

  public Resources withCpu(float ncpu) {
    this.cpu = ncpu;
    return this;
  }

  public Resources withRam(long nram) {
    this.ram = nram;
    return this;
  }

  public Resources withRamInMB(long nram) {
    return withRam(nram * 1024 * 1024);
  }

  public Resources withRamInGB(long nram) {
    return withRamInMB(nram * 1024);
  }

  @Override
  public String toString() {
    return "{ CPU: " + String.valueOf(cpu) + " RAM: " + String.valueOf(ram) + " }";
  }
}
