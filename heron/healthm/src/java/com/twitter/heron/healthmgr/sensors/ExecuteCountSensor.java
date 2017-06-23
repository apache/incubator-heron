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

import java.util.Map;
import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_EXE_COUNT;

public class ExecuteCountSensor extends BaseSensor {
  private final TopologyProvider topologyProvider;
  private final MetricsProvider metricsProvider;

  @Inject
  ExecuteCountSensor(TopologyProvider topologyProvider,
                     MetricsProvider metricsProvider) {
    super(METRIC_EXE_COUNT);
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
  }

  public Map<String, ComponentMetrics> get() {
    String[] boltNames = topologyProvider.getBoltNames();
    return get(boltNames);
  }

  public Map<String, ComponentMetrics> get(String... boltNames) {
    return metricsProvider.getComponentMetrics(this.metricName,
        HealthMgrConstants.DEFAULT_METRIC_DURATION,
        boltNames);
  }
}
