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

import com.microsoft.dhalion.api.ISensor;

import com.twitter.heron.healthmgr.HealthPolicyConfig;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.DEFAULT_METRIC_DURATION;

public abstract class BaseSensor implements ISensor {
  public static final String CONF_DURATION = ".duration";

  protected int duration = -1;
  protected HealthPolicyConfig config;
  protected String metricName;

  public BaseSensor(HealthPolicyConfig config, String metricName) {
    this.config = config;
    this.metricName = metricName;
  }

  /**
   * Returns the duration for which the metrics need to be collected
   *
   * @return duration in seconds
   */
  protected synchronized int getDuration(String prefix) {
    if (duration > 0) {
      return duration;
    }

    duration = DEFAULT_METRIC_DURATION;
    if (config == null) {
      return duration;
    }

    String configName = prefix + CONF_DURATION;
    String defaultValue = String.valueOf(DEFAULT_METRIC_DURATION);
    duration = Integer.parseInt(config.getConfig(configName, defaultValue));
    return duration;
  }

  public String getMetricName() {
    return metricName;
  }
}
