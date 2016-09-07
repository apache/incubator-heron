// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.packing;

/**
 * Definition of Resources. Used to form packing structure output.
 */
public class Resource {
  private double cpu;
  private long ram;
  private long disk;

  public Resource(double cpu, long ram, long disk) {
    this.cpu = cpu;
    this.ram = ram;
    this.disk = disk;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Resource) {
      Resource r = (Resource) obj;
      return (this.getCpu() == r.getCpu())
          && (this.getRam() == r.getRam())
          && (this.getDisk() == r.getDisk());
    } else {
      return false;
    }
  }

  public double getCpu() {
    return cpu;
  }

  public long getRam() {
    return ram;
  }

  public long getDisk() {
    return disk;
  }

  @Override
  public int hashCode() {
    return (Long.valueOf(getRam()).hashCode() << 2)
         & (Long.valueOf(getDisk()).hashCode() << 1)
         & (Double.valueOf(getCpu()).hashCode());
  }

  @Override
  public String toString() {
    return String.format("{cpu: %f, ram: %d, disk: %d}", getCpu(), getRam(), getDisk());
  }
}
