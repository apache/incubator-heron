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
package com.twitter.heron.scheduler.aurora;

/**
 * Field names passed to aurora controllers during job creation
 */
public enum AuroraField {
  CLUSTER,
  COMPONENT_JVM_OPTS_IN_BASE64,
  COMPONENT_RAMMAP,
  CORE_PACKAGE_URI,
  CPUS_PER_CONTAINER,
  DISK_PER_CONTAINER,
  ENVIRON,
  JAVA_HOME,
  INSTANCE_JVM_OPTS_IN_BASE64,
  TIER,
  NUM_CONTAINERS,
  RAM_PER_CONTAINER,
  ROLE,
  EXECUTOR_BINARY,
  INSTANCE_CLASSPATH,
  METRICSMGR_CLASSPATH,
  METRICS_YAML,
  PYTHON_INSTANCE_BINARY,
  SCHEDULER_CLASSPATH,
  SHELL_BINARY,
  STMGR_BINARY,
  SYSTEM_YAML,
  TMASTER_BINARY,
  STATEMGR_CONNECTION_STRING,
  STATEMGR_ROOT_PATH,
  TOPOLOGY_BINARY_FILE,
  TOPOLOGY_CLASSPATH,
  TOPOLOGY_DEFINITION_FILE,
  TOPOLOGY_ID,
  TOPOLOGY_NAME,
  TOPOLOGY_PACKAGE_TYPE,
  TOPOLOGY_PACKAGE_URI,
  METRICSCACHEMGR_CLASSPATH,
  IS_STATEFUL_ENABLED,
  CKPTMGR_CLASSPATH,
  STATEFUL_CONFIG_YAML
}
