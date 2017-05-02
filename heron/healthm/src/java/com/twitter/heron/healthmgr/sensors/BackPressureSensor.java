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

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;

public class BackPressureSensor extends BaseSensor {
  private final MetricsProvider metricsProvider;
  private final PackingPlanProvider packingPlanProvider;
  private final TopologyProvider topologyProvider;

  @Inject
  BackPressureSensor(PackingPlanProvider packingPlanProvider,
                     TopologyProvider topologyProvider,
                     MetricsProvider metricsProvider) {
    this.packingPlanProvider = packingPlanProvider;
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
  }

  @Override
  public Map<String, ComponentMetricsData> get(String... components) {
    return get();
  }

  public Map<String, ComponentMetricsData> get() {
    Map<String, ComponentMetricsData> result = new HashMap<>();

    String[] boltComponents = topologyProvider.getBoltNames();
    for (String boltComponent : boltComponents) {
      String[] boltInstanceNames = packingPlanProvider.getBoltInstanceNames(boltComponent);

      Map<String, InstanceMetricsData> instanceMetricsData = new HashMap<>();
      for (String boltInstanceName : boltInstanceNames) {
        String metric = HealthMgrConstants.METRIC_INSTANCE_BACK_PRESSURE + boltInstanceName;
        Map<String, ComponentMetricsData> stmgrResult = metricsProvider.getComponentMetrics(
            metric,
            HealthMgrConstants.DEFAULT_METRIC_DURATION,
            HealthMgrConstants.COMPONENT_STMGR);

        HashMap<String, InstanceMetricsData> streamManagerResult =
            stmgrResult.get(HealthMgrConstants.COMPONENT_STMGR).getMetrics();

        // since a bolt instance belongs to one stream manager, expect just one metrics
        // manager instance in the result
        InstanceMetricsData stmgrInstanceResult = streamManagerResult.values().iterator().next();

        InstanceMetricsData boltInstanceMetric = new InstanceMetricsData(boltInstanceName);

        boltInstanceMetric.addMetric(HealthMgrConstants.METRIC_INSTANCE_BACK_PRESSURE,
            stmgrInstanceResult.getMetricIntValue(metric));

        instanceMetricsData.put(boltInstanceName, boltInstanceMetric);
      }

      ComponentMetricsData componentMetricsData = new ComponentMetricsData(boltComponent,
          System.currentTimeMillis(),
          HealthMgrConstants.DEFAULT_METRIC_DURATION,
          instanceMetricsData);
      result.put(boltComponent, componentMetricsData);
    }

    return result;
  }
}
