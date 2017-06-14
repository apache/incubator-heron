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


package com.twitter.heron.healthmgr.sensors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;

public class BufferSizeSensor extends BaseSensor {
  private final MetricsProvider metricsProvider;
  private final PackingPlanProvider packingPlanProvider;
  private final TopologyProvider topologyProvider;

  @Inject
  BufferSizeSensor(PackingPlanProvider packingPlanProvider,
                   TopologyProvider topologyProvider,
                   MetricsProvider metricsProvider) {
    this.packingPlanProvider = packingPlanProvider;
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
  }

  @Override
  public Map<String, ComponentMetrics> get() {
    return get(topologyProvider.getBoltNames());
  }

  /**
   * The buffer size as provided by tracker
   * @param desiredBoltNames
   * @return buffer size
   */
  public Map<String, ComponentMetrics> get(String... desiredBoltNames) {
    Map<String, ComponentMetrics> result = new HashMap<>();

    Set<String> boltNameFilter = new HashSet<>();
    if (desiredBoltNames.length > 0) {
      boltNameFilter.addAll(Arrays.asList(desiredBoltNames));
    }

    String[] boltComponents = topologyProvider.getBoltNames();
    for (String boltComponent : boltComponents) {
      if (!boltNameFilter.isEmpty() && !boltNameFilter.contains(boltComponent)) {
        continue;
      }

      String[] boltInstanceNames = packingPlanProvider.getBoltInstanceNames(boltComponent);

      Map<String, InstanceMetrics> instanceMetrics = new HashMap<>();
      for (String boltInstanceName : boltInstanceNames) {
        String metric = BUFFER_SIZE + boltInstanceName + BUFFER_SIZE_SUFFIX;

        Map<String, ComponentMetrics> stmgrResult = metricsProvider.getComponentMetrics(
            metric,
            HealthMgrConstants.DEFAULT_METRIC_DURATION,
            HealthMgrConstants.COMPONENT_STMGR);

        HashMap<String, InstanceMetrics> streamManagerResult =
            stmgrResult.get(HealthMgrConstants.COMPONENT_STMGR).getMetrics();

        // since a bolt instance belongs to one stream manager, expect just one metrics
        // manager instance in the result
        double stmgrInstanceResult =
            streamManagerResult.values().iterator().next().getMetricValue(metric);

        InstanceMetrics boltInstanceMetric =
            new InstanceMetrics(boltInstanceName, BUFFER_SIZE, stmgrInstanceResult);

        instanceMetrics.put(boltInstanceName, boltInstanceMetric);
      }

      ComponentMetrics componentMetrics = new ComponentMetrics(boltComponent, instanceMetrics);
      result.put(boltComponent, componentMetrics);
    }

    return result;
  }
}
