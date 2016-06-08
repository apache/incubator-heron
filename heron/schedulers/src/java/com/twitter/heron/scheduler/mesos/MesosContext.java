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

package com.twitter.heron.scheduler.mesos;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Misc;

public class MesosContext extends Context {

  public static String frameworkZKEndpoint(Config config) {
    return config.getStringValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_ZOOKEEPER_ENDPOINT"), MesosDefaults.get(
            "HERON_MESOS_FRAMEWORK_ZOOKEEPER_ENDPOINT"));
  }

  public static String frameworkZKRoot(Config config) {
    return config.getStringValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_ZOOKEEPER_ROOT"), MesosDefaults.get(
            "HERON_MESOS_FRAMEWORK_ZOOKEEPER_ROOT"));
  }

  public static Integer frameworkZKConnectTimeout(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_ZOOKEEPER_CONNECT_TIMEOUT"), MesosDefaults
            .getInteger("HERON_MESOS_FRAMEWORK_ZOOKEEPER_CONNECT_TIMEOUT"));
  }

  public static Integer frameworkZKSessionTimeout(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_ZOOKEEPER_SESSION_TIMEOUT"), MesosDefaults
            .getInteger("HERON_MESOS_FRAMEWORK_ZOOKEEPER_SESSION_TIMEOUT"));
  }

  public static Long reconciliationIntervalMS(Config config) {
    return config.getLongValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_RECONCILIATION_INTERVAL_MS"), MesosDefaults.getLong(
            "HERON_MESOS_FRAMEWORK_RECONCILIATION_INTERVAL_MS"));
  }

  public static Integer failoverTimeoutSeconds(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("HERON_MESOS_FRAMEWORK_FAILOVER_TIMEOUT_SECONDS"), MesosDefaults.getInteger(
            "HERON_MESOS_FRAMEWORK_FAILOVER_TIMEOUT_SECONDS"));
  }

  public static String mesosMasterURI(Config config) {
    return config.getStringValue(
        MesosKeys.get("MESOS_MASTER_URI"), MesosDefaults.get("MESOS_MASTER_URI"));
  }

  public static Integer tmasterStatPort(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("TMASTER_STAT_PORT"), MesosDefaults.getInteger("TMASTER_STAT_PORT"));
  }

  public static Integer tmasterMainPort(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("TMASTER_MAIN_PORT"), MesosDefaults.getInteger("TMASTER_MAIN_PORT"));
  }

  public static Integer tmastercontrollerPort(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("TMASTER_CONTROLLER_PORT"), MesosDefaults.getInteger(
            "TMASTER_CONTROLLER_PORT"));
  }

  public static Integer tmasterShellPort(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("TMASTER_SHELL_PORT"), MesosDefaults.getInteger("TMASTER_SHELL_PORT"));
  }

  public static Integer tmasterMetricsMgrPort(Config config) {
    return config.getIntegerValue(
        MesosKeys.get("TMASTER_METRICSMGR_PORT"), MesosDefaults.getInteger(
            "TMASTER_METRICSMGR_PORT"));
  }

  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(
        MesosKeys.get("WORKING_DIRECTORY"), MesosDefaults.get("WORKING_DIRECTORY"));
    return Misc.substitute(config, workingDirectory);
  }

  public static boolean backgroundScheduler(Config config) {
    return config.getBooleanValue(
        MesosKeys.get("BACKGROUND_SCHEDULER"), MesosDefaults.getBoolean("BACKGROUND_SCHEDULER"));
  }
}
