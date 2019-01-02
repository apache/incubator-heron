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

package org.apache.heron.common.basics;

import java.util.HashMap;
import java.util.Map;

public final class CPUShare implements ResourceMeasure<CPUShare> {
  private final Double cpuShare;

  private CPUShare(Double cpuShare) {
    this.cpuShare = cpuShare;
  }

  public static CPUShare fromDouble(double share) {
    return new CPUShare(share);
  }

  public Double getCpuShare() {
    return cpuShare;
  }

  @Override
  public boolean isZero() {
    return cpuShare == 0.0;
  }

  @Override
  public CPUShare minus(CPUShare other) {
    return new CPUShare(cpuShare - other.cpuShare);
  }

  @Override
  public CPUShare plus(CPUShare other) {
    return new CPUShare(cpuShare + other.cpuShare);
  }

  @Override
  public CPUShare multiply(int factor) {
    return new CPUShare(cpuShare * factor);
  }

  @Override
  public CPUShare divide(int factor) {
    return new CPUShare(cpuShare / factor);
  }

  @Override
  public CPUShare increaseBy(int percentage) {
    return new CPUShare(cpuShare * (1.0 + percentage / 100.0));
  }

  @Override
  public boolean greaterThan(CPUShare other) {
    return cpuShare > other.cpuShare;
  }

  @Override
  public boolean greaterOrEqual(CPUShare other) {
    return cpuShare >= other.cpuShare;
  }

  @Override
  public boolean lessThan(CPUShare other) {
    return cpuShare < other.cpuShare;
  }

  @Override
  public boolean lessOrEqual(CPUShare other) {
    return cpuShare <= other.cpuShare;
  }

  public static Map<String, CPUShare> fromDoubleMap(Map<String, Double> doubleMap) {
    Map<String, CPUShare> retval = new HashMap<>();
    for (Map.Entry<String, Double> entry : doubleMap.entrySet()) {
      retval.put(entry.getKey(), new CPUShare(entry.getValue()));
    }
    return retval;
  }

  @Override
  public int compareTo(CPUShare o) {
    return Double.compare(cpuShare, o.cpuShare);
  }

  @Override
  public int hashCode() {
    return cpuShare.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    CPUShare that = (CPUShare) other;
    return cpuShare.equals(that.cpuShare);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
