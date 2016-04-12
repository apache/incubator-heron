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

import com.twitter.heron.scheduler.util.DefaultConfigLoader;

public class MesosConfig extends DefaultConfigLoader {
  public static final String HERON_MESOS_FRAMEWORK_ENDPOINT = "heron.mesos.framework.server.endpoint";
  public static final String HERON_MESOS_FRAMEWORK_ZOOKEEPER_ENDPOINT = "heron.mesos.framework.zookeeper.endpoint";
  public static final String HERON_MESOS_FRAMEWORK_ZOOKEEPER_ROOT = "heron.mesos.framework.zookeeper.root";
  public static final String HERON_MESOS_FRAMEWORK_ZOOKEEPER_CONNECT_TIMEOUT = "heron.mesos.framework.zookeeper.connect.timeout";
  public static final String HERON_MESOS_FRAMEWORK_ZOOKEEPER_SESSION_TIMEOUT = "heron.mesos.framework.zookeeper.session.timeout";
  public static final String HERON_MESOS_FRAMEWORK_RECONCILIATION_INTERVAL_MS = "heron.mesos.framework.reconciliation.interval.ms";
  public static final String HERON_MESOS_FRAMEWORK_FAILOVER_TIMEOUT_SECONDS = "heron.mesos.framework.failover.timeout.seconds";

  public static final String MESOS_MASTER_URI_PREFIX = "mesos.master.uri";

  public static final String MESOS_PKG_URI = "heron.mesos.pkg.uri";

  public static final String TMASTER_STAT_PORT = "tmaster.stat.port.nobase64";
  public static final String TMASTER_MAIN_PORT = "tmaster.main.port.nobase64";
  public static final String TMASTER_CONTROLLER_PORT = "tmaster.controller.port.nobase64";
  public static final String TMASTER_SHELL_PORT = "tmaster.shell.port.nobase64";
  public static final String TMASTER_METRICSMGR_PORT = "tmaster.metricsmgr.port.nobase64";

  public static final String BASE64_EQUALS = "&equals;";
}
