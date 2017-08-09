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

import java.time.Duration;

import com.microsoft.dhalion.api.ISensor;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey;

public abstract class BaseSensor implements ISensor {
  static final Duration DEFAULT_METRIC_DURATION = Duration.ofSeconds(300);
  static final String COMPONENT_STMGR = "__stmgr__";

  public enum MetricName {
    METRIC_EXE_COUNT("__execute-count/default"),
    METRIC_BACK_PRESSURE("__time_spent_back_pressure_by_compid/"),
    METRIC_BUFFER_SIZE("__connection_buffer_by_instanceid/"),
    METRIC_BUFFER_SIZE_SUFFIX("/packets"),
    METRIC_WAIT_Q_GROWTH_RATE("METRIC_WAIT_Q_GROWTH_RATE");

    private String text;

    MetricName(String name) {
      this.text = name;
    }

    public String text() {
      return text;
    }

    @Override
    public String toString() {
      return text();
    }
  }

  private Duration duration;
  private final HealthPolicyConfig config;
  private final String metricName;

  BaseSensor(HealthPolicyConfig config, String metricName, String confPrefix) {
    this.config = config;
    this.metricName = metricName;
    duration = getDurationFromConfig(confPrefix);
  }

  /**
   * Returns the duration for which the metrics need to be collected
   *
   * @return duration in seconds
   */
  protected synchronized Duration getDuration() {
    return duration;
  }

  private Duration getDurationFromConfig(String prefix) {
    Duration value = DEFAULT_METRIC_DURATION;

    String configName = prefix + PolicyConfigKey.CONF_SENSOR_DURATION_SUFFIX;
    if (config != null && config.getConfig(configName) != null) {
      value = Duration.ofSeconds((int) config.getConfig(configName));
    }

    return value;
  }

  public String getMetricName() {
    return metricName;
  }

  public HealthPolicyConfig getConfig() {
    return config;
  }
}
