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

package org.apache.heron.spi.packing;

public class ResourceUtility implements Comparable<ResourceUtility> {
  private final double ramUtility;
  private final double cpuUtility;

  public ResourceUtility(double ramUtility, double cpuUtility) {
    this.ramUtility = ramUtility;
    this.cpuUtility = cpuUtility;
  }

  public double getRamUtility() {
    return ramUtility;
  }

  public double getCpuUtility() {
    return cpuUtility;
  }

  @Override
  public String toString() {
    return String.format("RAM Utility=%.3f%%, CPU Utility=%.3f%%",
        ramUtility * 100, cpuUtility * 100);
  }

  @Override
  public int compareTo(ResourceUtility o) {
    if (ramUtility > o.ramUtility && cpuUtility > o.cpuUtility) {
      return 1;
    } else if (ramUtility < o.ramUtility && cpuUtility < o.cpuUtility) {
      return -1;
    } else {
      return 0;
    }
  }
}
