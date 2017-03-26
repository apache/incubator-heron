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
package org.apache.dhalion.metrics;

/**
 * An {@link MetricsInfo} holds information pertinent to a specific metric. For e.g. has latency
 * metric of a bolt instance for a Storm or Heron topology.
 */
public abstract class MetricsInfo {
  private String name;
  private MetricValue value;
  private long timestamp;
  private long duration;

  public String getName() {
    return name;
  }

  public MetricValue getValue() {
    return value;
  }

  public static class MetricValue implements Comparable<MetricValue> {
    Double value;

    public int asInt() {
      return value.intValue();
    }

    @Override
    public int compareTo(MetricValue o) {
      throw new UnsupportedOperationException("Needs to be implemented");
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null)
        return false;
      throw new UnsupportedOperationException("Needs to be implemented");
    }
  }
}
