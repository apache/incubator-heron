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
package org.apache.dhalion.symptom;

import java.util.Collection;

import org.apache.dhalion.metrics.MetricsInfo;

/**
 * {@link ComponentSymptom} represents a issue with a component in a distributed system. The issue
 * could be result of multiple {@link InstanceSymptom}s.
 */
public class ComponentSymptom extends Symptom {
  private ComponentInfo componentInfo;
  private Collection<InstanceSymptom> instanceSymptoms;

  public ComponentSymptom(ComponentInfo componentInfo) {
    this.componentInfo = componentInfo;
  }

  public void addInstanceSymptom(InstanceSymptom instanceSymptom) {
    instanceSymptoms.add(instanceSymptom);
  }

  @Override
  public String toString() {
    return "ComponentSymptom{" +
        "componentInfo=" + componentInfo +
        ", instancesSymptoms=" + instanceSymptoms +
        '}';
  }

  public ComponentInfo getComponentInfo() {
    return componentInfo;
  }

  public Collection<InstanceSymptom> getInstanceSymptoms() {
    return instanceSymptoms;
  }

  public boolean hasMetric(String metricName) {
    return hasMetric(metricName, null);
  }

  public boolean hasMetric(String metricName, MetricsInfo.MetricValue value) {
    return instanceSymptoms
        .stream()
        .filter(x -> x.hasMetric(metricName, value).isPresent())
        .findAny()
        .isPresent();
  }

  public boolean hasMetricBelowLimit(String metricName, MetricsInfo.MetricValue limit) {
    return instanceSymptoms
        .stream()
        .filter(x -> x.hasMetricBelowLimit(metricName, limit).isPresent())
        .findAny()
        .isPresent();
  }
}

