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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.core.Measurement;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;

public class ExecuteCountSensor extends BaseSensor {
  private final TopologyProvider topologyProvider;
  private final MetricsProvider metricsProvider;
  private Instant now;

  @Inject
  ExecuteCountSensor(TopologyProvider topologyProvider,
                     HealthPolicyConfig policyConfig,
                     MetricsProvider metricsProvider) {
    super(policyConfig, METRIC_EXE_COUNT.text(), ExecuteCountSensor.class.getSimpleName());
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
  }

  @Override
  public Collection<Measurement> fetch() {
    List<String> bolts = Arrays.asList(topologyProvider.getBoltNames());
    now = context.checkpoint();
    return metricsProvider.getMeasurements(now, getDuration(), getMetricTypes(), bolts);
  }
}
