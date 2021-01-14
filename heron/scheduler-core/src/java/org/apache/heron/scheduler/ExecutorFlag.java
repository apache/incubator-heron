/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler;

public enum ExecutorFlag {

  Shard("shard"),
  TopologyName("topology-name"),
  TopologyId("topology-id"),
  TopologyDefinitionFile("topology-defn-file"),
  StateManagerConnection("state-manager-connection"),
  StateManagerRoot("state-manager-root"),
  StateManagerConfigFile("state-manager-config-file"),
  TManagerBinary("tmanager-binary"),
  StmgrBinary("stmgr-binary"),
  MetricsManagerClasspath("metrics-manager-classpath"),
  InstanceJvmOpts("instance-jvm-opts"),
  Classpath("classpath"),
  ServerPort("server-port"),
  TManagerControllerPort("tmanager-controller-port"),
  TManagerStatsPort("tmanager-stats-port"),
  HeronInternalsConfigFile("heron-internals-config-file"),
  OverrideConfigFile("override-config-file"),
  ComponentRamMap("component-ram-map"),
  ComponentJvmOpts("component-jvm-opts"),
  PkgType("pkg-type"),
  TopologyBinaryFile("topology-binary-file"),
  HeronJavaHome("heron-java-home"),
  ShellPort("shell-port"),
  HeronShellBinary("heron-shell-binary"),
  MetricsManagerPort("metrics-manager-port"),
  Cluster("cluster"),
  Role("role"),
  Environment("environment"),
  InstanceClasspath("instance-classpath"),
  MetricsSinksConfigFile("metrics-sinks-config-file"),
  SchedulerClasspath("scheduler-classpath"),
  SchedulerPort("scheduler-port"),
  PythonInstanceBinary("python-instance-binary"),
  CppInstanceBinary("cpp-instance-binary"),
  MetricsCacheManagerClasspath("metricscache-manager-classpath"),
  MetricsCacheManagerServerPort("metricscache-manager-server-port"),
  MetricsCacheManagerStatsPort("metricscache-manager-stats-port"),
  MetricsCacheManagerMode("metricscache-manager-mode"),
  IsStateful("is-stateful"),
  CheckpointManagerClasspath("checkpoint-manager-classpath"),
  CheckpointManagerPort("checkpoint-manager-port"),
  CheckpointManagerRam("checkpoint-manager-ram"),
  StatefulConfigFile("stateful-config-file"),
  HealthManagerMode("health-manager-mode"),
  HealthManagerClasspath("health-manager-classpath"),
  EnableVerboseGCLog("verbose-gc"),
  JvmRemoteDebuggerPorts("jvm-remote-debugger-ports");

  private final String name;

  ExecutorFlag(String name) {
    this.name = name;
  }

  public String getFlag() {
    return String.format("--%s", name);
  }
}
