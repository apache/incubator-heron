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

package com.twitter.heron.healthmgr.common;

import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.detectors.LoadDisparityDetector;

public interface HealthMgrConstants {
  int DEFAULT_METRIC_DURATION = 300;

  String METRIC_EXE_COUNT = "__execute-count/default";
  String METRIC_BACK_PRESSURE = "__time_spent_back_pressure_by_compid/";
  String METRIC_BUFFER_SIZE = "__connection_buffer_by_instanceid/";
  String METRIC_BUFFER_SIZE_SUFFIX = "/packets";

  String COMPONENT_STMGR = "__stmgr__";

  String CONF_TRACKER_URL = "TRACKER_URL";
  String CONF_TOPOLOGY_NAME = "TOPOLOGY_NAME";
  String CONF_CLUSTER = "CLUSTER";
  String CONF_ENVIRON = "ENVIRON";

  // health policy configuration related keys
  String CONF_FILE_NAME = "healthmgr.yaml";
  String HEALTH_POLICIES = "heron.class.health.policies";
  String HEALTH_POLICY_CLASS = "health.policy.class";
  String HEALTH_POLICY_INTERVAL = "health.policy.interval.ms";

  // symtom names
  String SYMPTOM_BACK_PRESSURE = BackPressureDetector.class.getSimpleName();
  String SYMPTOM_LOAD_DISPARITY = LoadDisparityDetector.class.getSimpleName();
  
  void getType();
}
